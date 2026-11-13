use crate::protocol::{
    Error, Peer, Result, VERSION, random_hex, remaining, valid_id, validate_name,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Clone, Serialize, Deserialize)]
pub struct Descriptor {
    pub protocol: u32,
    pub peer: Peer,
    pub port: u16,
    pub token: String,
}

pub struct Registration {
    dir: PathBuf,
    lock: Option<File>,
    pub descriptor: Descriptor,
}

impl Drop for Registration {
    fn drop(&mut self) {
        remove_registration(&self.dir, self.lock.take());
    }
}

fn remove_registration(dir: &Path, lock: Option<File>) {
    let _ = fs::remove_file(dir.join("peer.json"));
    let _ = fs::remove_file(dir.join("peer.tmp"));
    drop(lock);
    let _ = fs::remove_file(dir.join("owner.lock"));
    let _ = fs::remove_dir(dir);
}

fn private_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.into()),
    }
    check_private(path, true)
}

fn check_private(path: &Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if (directory && !metadata.is_dir()) || (!directory && !metadata.is_file()) {
        return Err(Error::new(
            "unsafe_runtime",
            "Runtime entries must be regular files or directories",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // geteuid has no preconditions and does not access Rust memory.
        let uid = unsafe { libc::geteuid() };
        if metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
            return Err(Error::new(
                "unsafe_runtime",
                "Runtime entries must be private to the current user",
            ));
        }
    }
    Ok(())
}

fn file_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options
}

pub fn root() -> Result<PathBuf> {
    let path = if let Some(path) = std::env::var_os("INTERCOM_DIR") {
        PathBuf::from(path)
    } else {
        #[cfg(windows)]
        let base = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
        #[cfg(not(windows))]
        let base = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".cache")));
        let base = base.ok_or_else(|| {
            Error::new(
                "runtime",
                "Set INTERCOM_DIR to a private absolute directory",
            )
        })?;
        fs::create_dir_all(&base)?;
        base.join("agent-intercom")
    };
    if !path.is_absolute() {
        return Err(Error::new("runtime", "INTERCOM_DIR must be absolute"));
    }
    private_dir(&path)?;
    Ok(path)
}

pub fn register(root: &Path, name: String, port: u16) -> Result<Registration> {
    validate_name(&name)?;
    let id = random_hex(16)?;
    let token = random_hex(32)?;
    let dir = root.join(&id);
    // Fresh IDs are never reused, including during concurrent stale cleanup.
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&dir)?;
    let mut registration = Registration {
        dir,
        lock: None,
        descriptor: Descriptor {
            protocol: VERSION,
            peer: Peer { id, name },
            port,
            token,
        },
    };
    let lock = file_options()
        .create_new(true)
        .open(registration.dir.join("owner.lock"))?;
    lock.try_lock()
        .map_err(|_| Error::new("ownership", "Cannot acquire peer ownership"))?;
    registration.lock = Some(lock);
    let mut file = file_options()
        .create_new(true)
        .open(registration.dir.join("peer.tmp"))?;
    file.write_all(&serde_json::to_vec(&registration.descriptor)?)?;
    drop(file);
    fs::rename(
        registration.dir.join("peer.tmp"),
        registration.dir.join("peer.json"),
    )?;
    Ok(registration)
}

pub fn list(root: &Path, deadline: Instant) -> Result<Vec<Descriptor>> {
    let mut peers = Vec::new();
    for entry in fs::read_dir(root)? {
        remaining(deadline)?;
        let entry = entry?;
        let id = entry.file_name().to_string_lossy().into_owned();
        if !valid_id(&id) {
            continue;
        }
        let dir = entry.path();
        match read_live(&dir, &id) {
            Ok(Some(peer)) => peers.push(peer),
            Ok(None) => (),
            // A listener or another discoverer may have just removed it.
            Err(error) if error.code == "not_found" => (),
            Err(error) => return Err(error),
        }
    }
    peers.sort_by(|a, b| {
        a.peer
            .name
            .cmp(&b.peer.name)
            .then(a.peer.id.cmp(&b.peer.id))
    });
    Ok(peers)
}

fn read_live(dir: &Path, id: &str) -> Result<Option<Descriptor>> {
    check_private(dir, true)?;
    // No published descriptor means startup/teardown is in progress.
    if !dir.join("peer.json").exists() {
        return Ok(None);
    }
    check_private(&dir.join("owner.lock"), false)?;
    let lock = file_options().open(dir.join("owner.lock"))?;
    match lock.try_lock() {
        Ok(()) => {
            remove_registration(dir, Some(lock));
            return Ok(None);
        }
        Err(TryLockError::WouldBlock) => (),
        Err(TryLockError::Error(error)) => return Err(error.into()),
    }
    check_private(&dir.join("peer.json"), false)?;
    let file = file_options().open(dir.join("peer.json"))?;
    let mut data = Vec::new();
    file.take(4097).read_to_end(&mut data)?;
    if data.len() > 4096 {
        return Err(Error::new("invalid_peer", "Peer descriptor is too large"));
    }
    let peer: Descriptor = serde_json::from_slice(&data)?;
    if peer.peer.id != id || peer.token.len() != 64 || peer.port == 0 {
        return Err(Error::new("invalid_peer", "Invalid peer descriptor"));
    }
    validate_name(&peer.peer.name)?;
    Ok(Some(peer))
}

pub fn resolve(root: &Path, target: &str, deadline: Instant) -> Result<Descriptor> {
    if target.len() < 6 || target.len() > 32 || !target.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::new(
            "invalid_peer",
            "Use a peer ID or an unambiguous prefix of at least 6 characters",
        ));
    }
    let mut matches = list(root, deadline)?
        .into_iter()
        .filter(|d| d.peer.id.starts_with(target));
    let peer = matches
        .next()
        .ok_or_else(|| Error::new("not_found", "Peer is no longer registered"))?;
    if matches.next().is_some() {
        return Err(Error::new("ambiguous_peer", "Peer ID prefix is ambiguous"));
    }
    if peer.protocol != VERSION {
        return Err(Error::new("protocol", "Peer uses an incompatible protocol"));
    }
    Ok(peer)
}
