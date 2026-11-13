use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

pub const VERSION: u32 = 1;
pub const TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_FRAME: usize = 128 * 1024;
pub const MAX_TEXT: usize = 16 * 1024;
pub const MAX_PENDING: usize = 64;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize)]
pub struct Error {
    pub code: String,
    pub message: String,
}

impl Error {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        let code = match error.kind() {
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => "timeout",
            std::io::ErrorKind::NotFound => "not_found",
            _ => "io",
        };
        Self::new(code, error.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::new("invalid_json", error.to_string())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub from: Option<Peer>,
    pub text: String,
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub protocol: u32,
    pub token: String,
    pub from: Option<String>,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Reply {
    Accepted { id: String },
    Error { code: String, message: String },
}

impl From<Error> for Reply {
    fn from(error: Error) -> Self {
        Self::Error {
            code: error.code,
            message: error.message,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Ready { protocol: u32, peer: Peer },
    Message { message: Message },
}

pub fn random_hex(bytes: usize) -> Result<String> {
    let mut data = vec![0; bytes];
    getrandom::fill(&mut data).map_err(|_| Error::new("random", "OS randomness is unavailable"))?;
    Ok(data.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn validate_name(name: &str) -> Result<()> {
    if name.trim().is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
        return Err(Error::new(
            "invalid_name",
            "Name must be printable and contain 1–128 bytes",
        ));
    }
    Ok(())
}

pub fn validate_text(text: &str) -> Result<()> {
    if text.trim().is_empty() || text.len() > MAX_TEXT {
        return Err(Error::new(
            "invalid_message",
            "Message must contain 1–16384 bytes of text",
        ));
    }
    Ok(())
}

pub fn remaining(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| Error::new("timeout", "Request timed out"))
}

pub fn write_frame(
    stream: &mut TcpStream,
    value: &impl Serialize,
    deadline: Instant,
) -> Result<()> {
    let data = serde_json::to_vec(value)?;
    if data.len() > MAX_FRAME {
        return Err(Error::new("too_large", "Frame exceeds 128 KiB"));
    }
    let mut frame = Vec::with_capacity(4 + data.len());
    frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
    frame.extend_from_slice(&data);
    let mut offset = 0;
    while offset < frame.len() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        let count = stream.write(&frame[offset..])?;
        if count == 0 {
            return Err(Error::new("closed", "Peer closed the connection"));
        }
        offset += count;
    }
    Ok(())
}

pub fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream, deadline: Instant) -> Result<T> {
    fn read(stream: &mut TcpStream, data: &mut [u8], deadline: Instant) -> Result<()> {
        let mut offset = 0;
        while offset < data.len() {
            stream.set_read_timeout(Some(remaining(deadline)?))?;
            let count = stream.read(&mut data[offset..])?;
            if count == 0 {
                return Err(Error::new("closed", "Peer closed the connection"));
            }
            offset += count;
        }
        Ok(())
    }
    let mut header = [0; 4];
    read(stream, &mut header, deadline)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME {
        return Err(Error::new("too_large", "Frame exceeds 128 KiB"));
    }
    let mut data = vec![0; length];
    read(stream, &mut data, deadline)?;
    Ok(serde_json::from_slice(&data)?)
}
