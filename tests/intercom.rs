use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Fixture(PathBuf);
static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "intercom-test-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        private_dir(&path);
        Self(path)
    }

    fn root(&self) -> PathBuf {
        self.0.join("runtime")
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_intercom"));
        command
            .env("INTERCOM_DIR", self.root())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut child = self.command().args(args).spawn().unwrap();
        wait(&mut child, Duration::from_secs(5));
        child.wait_with_output().unwrap()
    }

    fn json(&self, args: &[&str]) -> Value {
        let output = self.run(&[&["--json"], args].concat());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn listen(&self, name: &str) -> PeerProcess {
        let mut child = self
            .command()
            .args(["listen", "--name", name, "--stdio"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, events) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let Ok(event) = serde_json::from_str(&line) else {
                    break;
                };
                if sender.send(event).is_err() {
                    break;
                }
            }
        });
        let ready: Value = events
            .recv_timeout(Duration::from_secs(3))
            .expect("receiver readiness");
        assert_eq!(ready["event"], "ready");
        PeerProcess {
            child,
            events,
            id: ready["peer"]["id"].as_str().unwrap().into(),
        }
    }

    fn descriptor(&self, id: &str) -> Value {
        serde_json::from_slice(&fs::read(self.root().join(id).join("peer.json")).unwrap()).unwrap()
    }

    fn own_peer(&self, id: &str, port: u16) -> File {
        if !self.root().exists() {
            private_dir(&self.root());
        }
        let dir = self.root().join(id);
        private_dir(&dir);
        let lock = private_file(&dir.join("owner.lock"));
        lock.lock().unwrap();
        let descriptor = json!({"protocol":1,"peer":{"id":id,"name":"fixture"},"port":port,"token":"a".repeat(64)});
        private_file(&dir.join("peer.json"))
            .write_all(descriptor.to_string().as_bytes())
            .unwrap();
        lock
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct PeerProcess {
    child: Child,
    events: Receiver<Value>,
    id: String,
}

impl Drop for PeerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn private_dir(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path).unwrap();
    }
    #[cfg(not(unix))]
    fs::create_dir(path).unwrap();
}

fn private_file(path: &Path) -> File {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap()
}

fn wait(child: &mut Child, limit: Duration) {
    let start = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if start.elapsed() > limit {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child exceeded {limit:?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn connect(descriptor: &Value) -> TcpStream {
    let stream =
        TcpStream::connect(("127.0.0.1", descriptor["port"].as_u64().unwrap() as u16)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
}

fn rpc(descriptor: &Value, mut request: Value) -> Value {
    if request.get("token").is_none() {
        request["token"] = descriptor["token"].clone();
    }
    let mut stream = connect(descriptor);
    let data = serde_json::to_vec(&request).unwrap();
    stream
        .write_all(&(data.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&data).unwrap();
    read_reply(&mut stream)
}

fn read_reply(stream: &mut TcpStream) -> Value {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let size = u32::from_be_bytes(header) as usize;
    assert!(size <= 128 * 1024);
    let mut data = vec![0; size];
    stream.read_exact(&mut data).unwrap();
    serde_json::from_slice(&data).unwrap()
}

#[test]
fn discovery_delivery_and_reply_share_peer_identity() {
    let fixture = Fixture::new();
    let alice = fixture.listen("api");
    let bob = fixture.listen("review");
    let peers = fixture.json(&["list"]);
    assert_eq!(peers["peers"].as_array().unwrap().len(), 2);
    assert!(!peers.to_string().contains("token"));
    let accepted = fixture.json(&["send", &bob.id[..8], "Ready 🦀", "--from", &alice.id]);
    assert_eq!(accepted["status"], "accepted");
    let event = bob.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event["message"]["id"], accepted["id"]);
    assert_eq!(
        event["message"]["from"],
        json!({"id": alice.id, "name": "api"})
    );
    assert_eq!(event["message"]["text"], "Ready 🦀");
    fixture.json(&["send", &alice.id, "Reviewed", "--from", &bob.id]);
    assert_eq!(
        alice.events.recv_timeout(Duration::from_secs(2)).unwrap()["message"]["text"],
        "Reviewed"
    );
}

#[test]
fn message_text_is_not_parsed_as_a_flag_or_persisted() {
    let fixture = Fixture::new();
    let peer = fixture.listen("api");
    fixture.json(&["send", &peer.id, "--json"]);
    let event = peer.events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event["message"]["text"], "--json");
    let entries: Vec<_> = fs::read_dir(fixture.root().join(&peer.id))
        .unwrap()
        .collect();
    assert_eq!(entries.len(), 2);
    let descriptor = fixture.descriptor(&peer.id);
    assert!(!descriptor.to_string().contains("--json"));
}

#[test]
fn stdin_eof_removes_registration_without_waiting_for_clients() {
    let fixture = Fixture::new();
    let mut peer = fixture.listen("api");
    let descriptor = fixture.descriptor(&peer.id);
    let _stalled_client = connect(&descriptor);
    let start = Instant::now();
    drop(peer.child.stdin.take());
    wait(&mut peer.child, Duration::from_secs(1));
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(!fixture.root().join(&peer.id).exists());
}

#[test]
fn crash_cleanup_and_replacement_do_not_reuse_identity() {
    let fixture = Fixture::new();
    let mut old = fixture.listen("api");
    old.child.kill().unwrap();
    old.child.wait().unwrap();
    assert!(fixture.root().join(&old.id).exists());
    assert_eq!(fixture.json(&["list"])["peers"], json!([]));
    assert!(!fixture.root().join(&old.id).exists());
    let replacement = fixture.listen("api");
    assert_ne!(old.id, replacement.id);
    let output = fixture.run(&["--json", "send", &old.id, "Old message"]);
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "not_found"
    );
}

#[test]
fn same_name_peers_remain_independently_addressable() {
    let fixture = Fixture::new();
    let a = fixture.listen("api");
    let b = fixture.listen("api");
    assert_ne!(a.id, b.id);
    assert_eq!(
        fixture.json(&["list"])["peers"].as_array().unwrap().len(),
        2
    );
    fixture.json(&["send", &a.id, "A"]);
    assert_eq!(
        a.events.recv_timeout(Duration::from_secs(2)).unwrap()["message"]["text"],
        "A"
    );
    assert!(b.events.try_recv().is_err());
}

#[test]
fn invalid_requests_are_rejected_before_delivery() {
    let fixture = Fixture::new();
    let peer = fixture.listen("api");
    let descriptor = fixture.descriptor(&peer.id);
    for (request, code) in [
        (
            json!({"protocol":1,"token":"wrong","text":"hello"}),
            "unauthorized",
        ),
        (json!({"protocol":2,"text":"hello"}), "protocol"),
        (json!({"protocol":1,"text":" "}), "invalid_message"),
        (
            json!({"protocol":1,"text":"x".repeat(16385)}),
            "invalid_message",
        ),
        (
            json!({"protocol":1,"from":"../../escape","text":"hello"}),
            "invalid_peer",
        ),
    ] {
        assert_eq!(rpc(&descriptor, request)["code"], code);
    }
    let mut stream = connect(&descriptor);
    stream
        .write_all(&((128 * 1024 + 1) as u32).to_be_bytes())
        .unwrap();
    assert_eq!(read_reply(&mut stream)["code"], "too_large");
    assert!(peer.events.try_recv().is_err());
    fixture.json(&["send", &peer.id, "Still alive"]);
}

#[test]
fn discovery_does_not_wait_for_an_unresponsive_owned_endpoint() {
    let fixture = Fixture::new();
    private_dir(&fixture.root());
    let id = "a".repeat(32);
    let dir = fixture.root().join(&id);
    private_dir(&dir);
    let lock = private_file(&dir.join("owner.lock"));
    lock.lock().unwrap();
    let server = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let descriptor = json!({"protocol":1,"peer":{"id":id,"name":"stalled"},"port":server.local_addr().unwrap().port(),"token":"a".repeat(64)});
    private_file(&dir.join("peer.json"))
        .write_all(descriptor.to_string().as_bytes())
        .unwrap();
    let start = Instant::now();
    assert_eq!(fixture.json(&["list"])["peers"][0]["name"], "stalled");
    assert!(start.elapsed() < Duration::from_secs(1));
    let output = fixture.run(&["--json", "send", &id, "hello"]);
    assert!(!output.status.success());
    assert!(start.elapsed() < Duration::from_secs(4));
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "timeout"
    );
    assert!(dir.exists());
}

#[test]
fn queue_backpressure_and_shutdown_with_unread_stdout_are_bounded() {
    let fixture = Fixture::new();
    let mut child = fixture
        .command()
        .args(["listen", "--name", "blocked", "--stdio"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut ready = String::new();
    stdout.read_line(&mut ready).unwrap();
    let ready: Value = serde_json::from_str(&ready).unwrap();
    let id = ready["peer"]["id"].as_str().unwrap();
    let descriptor = fixture.descriptor(id);
    let mut busy = false;
    for _ in 0..100 {
        let reply = rpc(&descriptor, json!({"protocol":1,"text":"x".repeat(16384)}));
        if reply["code"] == "busy" {
            busy = true;
            break;
        }
        assert_eq!(reply["status"], "accepted");
    }
    assert!(busy, "receiver must bound its pending output");
    drop(child.stdin.take());
    wait(&mut child, Duration::from_secs(1));
    assert!(!fixture.root().join(id).exists());
}

#[cfg(unix)]
#[test]
fn runtime_rejects_public_permissions_and_symlinks() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let fixture = Fixture::new();
    private_dir(&fixture.root());
    fs::set_permissions(fixture.root(), fs::Permissions::from_mode(0o755)).unwrap();
    let output = fixture.run(&["--json", "list"]);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "unsafe_runtime"
    );
    fs::remove_dir(fixture.root()).unwrap();
    let elsewhere = fixture.0.join("elsewhere");
    private_dir(&elsewhere);
    symlink(&elsewhere, fixture.root()).unwrap();
    let output = fixture.run(&["--json", "list"]);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "unsafe_runtime"
    );
}

#[test]
fn endpoint_reuse_cannot_deliver_to_a_different_receiver() {
    let fixture = Fixture::new();
    let a = fixture.listen("api");
    let b = fixture.listen("review");
    let mut descriptor = fixture.descriptor(&a.id);
    descriptor["port"] = fixture.descriptor(&b.id)["port"].clone();
    fs::write(
        fixture.root().join(&a.id).join("peer.json"),
        descriptor.to_string(),
    )
    .unwrap();
    let output = fixture.run(&["--json", "send", &a.id, "Wrong endpoint"]);
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "unauthorized"
    );
    assert!(b.events.try_recv().is_err());
}

#[test]
fn slow_partial_frames_expire_and_release_connection_slots() {
    let fixture = Fixture::new();
    let peer = fixture.listen("api");
    let descriptor = fixture.descriptor(&peer.id);
    let mut stream = connect(&descriptor);
    for _ in 0..3 {
        stream.write_all(&[0]).unwrap();
        thread::sleep(Duration::from_millis(750));
    }
    let mut byte = [0];
    assert_eq!(stream.read(&mut byte).unwrap(), 0);
    fixture.json(&["send", &peer.id, "After deadline"]);
    assert_eq!(
        peer.events.recv_timeout(Duration::from_secs(2)).unwrap()["message"]["text"],
        "After deadline"
    );
}

#[test]
fn full_ids_isolate_delivery_from_unrelated_invalid_registrations() {
    let fixture = Fixture::new();
    let a = fixture.listen("api");
    let b = fixture.listen("review");
    let invalid = "f".repeat(32);
    let _owner = fixture.own_peer(&invalid, 9);
    fs::write(fixture.root().join(&invalid).join("peer.json"), "{").unwrap();
    fixture.json(&["send", &b.id.to_ascii_uppercase(), "Ready", "--from", &a.id]);
    assert_eq!(
        b.events.recv_timeout(Duration::from_secs(2)).unwrap()["message"]["text"],
        "Ready"
    );
}

#[test]
fn ambiguous_prefixes_fail_before_contacting_a_receiver() {
    let fixture = Fixture::new();
    let _a = fixture.own_peer(&format!("abcdef{}", "0".repeat(26)), 9);
    let _b = fixture.own_peer(&format!("abcdef{}", "1".repeat(26)), 9);
    let output = fixture.run(&["--json", "send", "abcdef", "Ready"]);
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "ambiguous_peer"
    );
}

#[cfg(unix)]
#[test]
fn runtime_defaults_ignore_empty_paths_and_reject_relative_paths() {
    let fixture = Fixture::new();
    let mut command = fixture.command();
    command
        .env_remove("INTERCOM_DIR")
        .env("HOME", &fixture.0)
        .env("XDG_RUNTIME_DIR", "")
        .args(["--json", "list"]);
    let mut child = command.spawn().unwrap();
    wait(&mut child, Duration::from_secs(5));
    assert!(child.wait_with_output().unwrap().status.success());
    assert!(fixture.0.join(".cache/agent-intercom").is_dir());

    let mut command = fixture.command();
    command
        .env_remove("INTERCOM_DIR")
        .env("HOME", &fixture.0)
        .env("XDG_RUNTIME_DIR", "relative")
        .current_dir(&fixture.0)
        .args(["--json", "list"]);
    let mut child = command.spawn().unwrap();
    wait(&mut child, Duration::from_secs(5));
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["error"]["code"],
        "runtime"
    );
    assert!(!fixture.0.join("relative").exists());
}
