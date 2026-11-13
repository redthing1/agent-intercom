# Development

- Keep transport and discovery in the CLI. Host adapters own agent lifecycle and input delivery.
- Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, and `cargo test --locked` after code changes.
- Test with isolated runtime directories and synthetic messages.
- Keep public documentation concise. Implementation notes belong in ignored `notes/`.
