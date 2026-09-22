# opcdapter

OPC DA over gRPC: a Java/Utgard server and a Go client in one repository.

## Layout

- `proto/` — canonical `opcda.v1` protocol.
- `gen/go/` — generated Go bindings.
- `client-go/` — Go client package (`package client`).
- `server/` — Java server, generated Java bindings, Gradle build, and Docker image.

The Go module is `github.com/dalugm/opcdapter`; import the client with:

```go
import "github.com/dalugm/opcdapter/client-go"
```

## Development

```bash
just gen       # regenerate Go and Java bindings
just go-test   # test Go packages
just build     # compile Java
just test      # run Java tests
just lint      # check Java formatting
```

Buf remote plugins are version-pinned in `buf.gen.yaml`. Generated sources are
committed; do not edit them manually. Runtime dependencies are managed by the
Micronaut Platform BOM.

Live OPC DA tests are opt-in and require `OPCDA_HOST` plus exactly one of
`OPCDA_CLS_ID` or `OPCDA_PROG_ID`:

```bash
OPCDA_HOST=... OPCDA_PROG_ID=... just opcda-time
OPCDA_HOST=... OPCDA_PROG_ID=... OPCDA_ITEMS=A.PV,B.PV just opcda-read
OPCDA_HOST=... OPCDA_PROG_ID=... OPCDA_PAIRS=A.PV=1 just opcda-write
```

Never run live tests against an unintended device, and keep OPC credentials out
of logs and committed files.

## Docker

Build an offline-loadable Linux/amd64 image:

```bash
just docker
docker load -i opcdapter-v0.5.1.tar
docker run -d --name opcdapter \
  -p 127.0.0.1:50051:50051 \
  -v opcdapter-logs:/app/logs \
  opcdapter:v0.5.1
```

Set `OPCDAPTER_IMAGE` when publishing to a registry, for example
`ghcr.io/dalugm/opcdapter`.

The gRPC server listens on `127.0.0.1:50051` by default. If it must be
reachable remotely, configure an explicit auth token and TLS, then restrict
port `50051` with the host firewall. The container health endpoint is internal
on port `8080`.
