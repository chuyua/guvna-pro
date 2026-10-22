# Guvna — multi-stage build to a distroless static image.
# Result: ~19MB, no shell, no package manager, runs as nonroot (UID 65532).

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/guvna ./cmd/guvna \
 && mkdir -p /data && chown 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/guvna /guvna
COPY --chown=65532:65532 --from=build /data /data
COPY config.yaml /config.yaml
EXPOSE 20128
USER nonroot
ENTRYPOINT ["/guvna", "-config", "/config.yaml", "-data", "/data"]
