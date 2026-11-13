# Host integration

Start `intercom listen --name <label> --stdio` as an owned subprocess with piped
stdin/stdout. Keep stdin open for its lifetime. Read UTF-8 JSON lines from stdout;
stderr contains diagnostics. Wait for readiness before exposing messaging:

```json
{"event":"ready","protocol":1,"peer":{"id":"…","name":"api"}}
```

Require protocol 1. The peer ID identifies this receiver incarnation; labels need
not be unique. Each incoming event has this shape:

```json
{"event":"message","message":{"id":"…","from":{"id":"…","name":"review"},"text":"Ready."}}
```

`from` is `null` for unregistered senders. Messages are limited to 16 KiB of UTF-8
text. Treat peer content as attributed input under the receiving session's existing
permissions. Deliver through the host's normal event queue and idle wakeup.

Invoke `intercom --json list` and `intercom --json send <peer> <text> --from <self>`
as local subprocesses without a shell. Results are single JSON objects. Errors
return a nonzero exit code and `{"error":{"code":"…","message":"…"}}`.
Successful sends return `{"status":"accepted","id":"…"}`.

Bound the host's pending queue and stop reading when full. The listener buffers
up to 64 output events, then returns `busy` to senders. Messages are ephemeral;
acceptance does not confirm delivery into the host or model processing. A timeout
can leave acceptance uncertain; do not retry automatically.

Close stdin on shutdown or session replacement. Stop exposing tools if the
receiver exits. Bound startup and shutdown waits, and terminate only the receiver
you own if it fails to exit. Workspace remoting must keep these operations on the
controller that owns the receiver.

Discovery defaults to `XDG_RUNTIME_DIR/agent-intercom` or
`~/.cache/agent-intercom` on Unix, and `LOCALAPPDATA/agent-intercom` on Windows.
`INTERCOM_DIR` selects another private absolute directory. All participating
processes must use the same directory.
