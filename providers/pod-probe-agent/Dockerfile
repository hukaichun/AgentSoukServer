# Build context is this directory. Unlike the Python providers, this image
# copies nothing from the rest of the repo: the agent speaks the wire
# directly (docs/server-mode.md) and depends on no SDK, so there is nothing
# else to bring in. The wire-vectors test that proves byte-exactness runs in
# CI against docs/wire-vectors.json, not in this build.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
# Static, stripped, no cgo — the single artifact that gets dropped into a
# pod. CGO off is what makes it run on scratch and on a distroless image
# with no libc.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /pod-probe .

# scratch: the smallest possible surface in the pod. No shell, no package
# manager, no second binary — which is also the posture the agent inspects
# for, and cannot itself be used to change anything. /data is where an
# identity key persists if a volume is mounted there; without one, a fresh
# identity is minted each start (a restarted pod is a new stall).
FROM scratch
COPY --from=build /pod-probe /pod-probe
VOLUME /data
ENTRYPOINT ["/pod-probe"]
