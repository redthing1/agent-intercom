mod protocol;
mod receiver;
mod registry;

use protocol::{Error, Result};
use serde_json::json;

const HELP: &str = "intercom — local messaging between independent agents

Usage:
  intercom listen --name <name> --stdio
  intercom [--json] list
  intercom [--json] send <peer-id> <text> [--from <peer-id>]
  intercom [--json] --version

listen emits JSON lines until stdin closes. Keep its input pipe open.
Peer IDs accept unambiguous prefixes of at least 6 characters.
INTERCOM_DIR selects a private absolute runtime directory.
";

fn run(mut args: Vec<String>) -> Result<()> {
    let json_output = args.first().is_some_and(|a| a == "--json");
    if json_output {
        args.remove(0);
    }
    let command = args.first().map(String::as_str).unwrap_or("--help");
    match command {
        "--help" | "-h" | "help" => print!("{HELP}"),
        "--version" if args.len() == 1 => {
            if json_output {
                println!(
                    "{}",
                    json!({"version": env!("CARGO_PKG_VERSION"), "protocol": protocol::VERSION})
                );
            } else {
                println!("intercom {}", env!("CARGO_PKG_VERSION"));
            }
        }
        "listen" if args.len() == 4 && args[1] == "--name" && args[3] == "--stdio" => {
            receiver::listen(&registry::root()?, args[2].clone())?;
        }
        "list" if args.len() == 1 => {
            let peers: Vec<_> = registry::list(
                &registry::root()?,
                std::time::Instant::now() + protocol::TIMEOUT,
            )?
            .into_iter()
            .map(|d| d.peer)
            .collect();
            if json_output {
                println!("{}", json!({"peers": peers}));
            } else if peers.is_empty() {
                println!("No peers are registered.");
            } else {
                for peer in peers {
                    println!("{}  {}", peer.id, peer.name);
                }
            }
        }
        "send" if args.len() == 3 || (args.len() == 5 && args[3] == "--from") => {
            let from = args.get(4).cloned();
            let reply = receiver::send(&registry::root()?, &args[1], from, args[2].clone())?;
            if json_output {
                println!("{}", serde_json::to_string(&reply)?);
            } else if let protocol::Reply::Accepted { id } = reply {
                println!("Accepted {id}");
            }
        }
        _ => {
            return Err(Error::new(
                "usage",
                "Invalid arguments. Run intercom --help.",
            ));
        }
    }
    Ok(())
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let json_output = args.first().is_some_and(|a| a == "--json");
    if let Err(error) = run(args) {
        if json_output {
            println!("{}", json!({"error": error}));
        } else {
            eprintln!("intercom: {error}");
        }
        std::process::exit(1);
    }
}
