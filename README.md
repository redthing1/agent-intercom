# agent-intercom

Local messaging between independent agents.

## Install

```sh
cargo install --path . --locked
```

## Use

```sh
intercom listen --name api --stdio
```

In another terminal:

```sh
intercom list
intercom send <peer-id> "The schema is ready."
```

`listen` streams JSON messages until stdin closes. Add `--json` before `list` or
`send` for structured results. Run `intercom --help` for command details.

[Pi extension](https://github.com/redthing1/pi-extensions/tree/main/extensions/intercom)
· [Agent skill](skills/intercom/SKILL.md)
· [Host integration](docs/hosts.md)
