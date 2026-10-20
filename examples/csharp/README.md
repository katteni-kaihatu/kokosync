# C# console example

With core and sidecar running:

```powershell
.tools/dotnet/dotnet.exe run --project examples/csharp -- --insecure-development --smoke --room mixed --peer csharp
```

The example joins with optional state enabled, sends ordered messaging, state,
an event, latest position, and a small live byte frame, then prints received frames.
`--smoke` exits after 12 seconds (`--seconds` overrides this). Omit it for an interactive
session. Use `--websocket` to exercise the WebSocket fallback instead.
