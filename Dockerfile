# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/cluster-node ./cmd/cluster-node

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/cluster-node /usr/local/bin/cluster-node
COPY data/SMSSpamCollection /app/data/SMSSpamCollection

# Pre-create the shared partitions mount point (/data/partitions, mounted by
# docker-compose.yml) owned by app: Docker seeds a named volume's initial
# content and permissions from whatever already exists at the mount path in
# the image, the first time it's mounted.
RUN mkdir -p /data/partitions && chown -R app:app /data /app/data

USER app
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/cluster-node"]
