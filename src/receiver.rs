use crate::protocol::{self, Error, Event, Message, Reply, Request, Result, TIMEOUT, VERSION};
use crate::registry::{self, Descriptor};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

pub fn send(root: &Path, target: &str, from: Option<String>, text: String) -> Result<Reply> {
    protocol::validate_text(&text)?;
    let deadline = Instant::now() + TIMEOUT;
    let peer = registry::resolve(root, target, deadline)?;
    let from = from
        .map(|id| registry::resolve(root, &id, deadline).map(|p| p.peer.id))
        .transpose()?;
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, peer.port));
    let mut stream = TcpStream::connect_timeout(&address, protocol::remaining(deadline)?)
        .map_err(|_| Error::new("unavailable", "Peer is unavailable"))?;
    protocol::write_frame(
        &mut stream,
        &Request {
            protocol: VERSION,
            token: peer.token,
            from,
            text,
        },
        deadline,
    )?;
    match protocol::read_frame(&mut stream, deadline)? {
        Reply::Error { code, message } => Err(Error { code, message }),
        reply => Ok(reply),
    }
}

pub fn listen(root: &Path, name: String) -> Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    listener.set_nonblocking(true)?;
    let registration = registry::register(root, name, listener.local_addr()?.port())?;
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    thread::spawn(move || {
        let mut buffer = [0; 1024];
        let mut input = std::io::stdin().lock();
        while let Ok(count) = input.read(&mut buffer) {
            if count == 0 {
                break;
            }
        }
        stop.store(true, Ordering::Release);
    });
    let (output, events) = mpsc::sync_channel(protocol::MAX_PENDING);
    output
        .send(Event::Ready {
            protocol: VERSION,
            peer: registration.descriptor.peer.clone(),
        })
        .map_err(|_| Error::new("closed", "Receiver output closed"))?;
    let stop = stopped.clone();
    thread::spawn(move || {
        let mut stdout = std::io::stdout().lock();
        for event in events {
            let result = serde_json::to_writer(&mut stdout, &event)
                .map_err(Error::from)
                .and_then(|_| {
                    stdout.write_all(b"\n")?;
                    stdout.flush()?;
                    Ok(())
                });
            if result.is_err() {
                stop.store(true, Ordering::Release);
                break;
            }
        }
    });
    let clients = Arc::new(AtomicUsize::new(0));
    while !stopped.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                // Accepted sockets inherit nonblocking mode on some platforms.
                stream.set_nonblocking(false)?;
                if clients.load(Ordering::Acquire) >= 8 {
                    continue;
                }
                clients.fetch_add(1, Ordering::AcqRel);
                let clients = clients.clone();
                let descriptor = registration.descriptor.clone();
                let root = root.to_path_buf();
                let output = output.clone();
                thread::spawn(move || {
                    let deadline = Instant::now() + TIMEOUT;
                    let reply = accept_message(&mut stream, &root, &descriptor, &output, deadline)
                        .unwrap_or_else(Reply::from);
                    let _ = protocol::write_frame(&mut stream, &reply, deadline);
                    clients.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(error) => return Err(error.into()),
        }
    }
    // Workers never own the lifetime lock. Process exit does not join blocked I/O.
    Ok(())
}

fn accept_message(
    stream: &mut TcpStream,
    root: &Path,
    descriptor: &Descriptor,
    output: &mpsc::SyncSender<Event>,
    deadline: Instant,
) -> Result<Reply> {
    let request: Request = protocol::read_frame(stream, deadline)?;
    if request.token != descriptor.token {
        return Err(Error::new("unauthorized", "Invalid receiver credential"));
    }
    if request.protocol != VERSION {
        return Err(Error::new("protocol", "Incompatible protocol"));
    }
    protocol::validate_text(&request.text)?;
    let from = request
        .from
        .map(|id| registry::resolve(root, &id, deadline).map(|d| d.peer))
        .transpose()?;
    let id = protocol::random_hex(16)?;
    output
        .try_send(Event::Message {
            message: Message {
                id: id.clone(),
                from,
                text: request.text,
            },
        })
        .map_err(|_| Error::new("busy", "Receiver queue is full or closed"))?;
    Ok(Reply::Accepted { id })
}
