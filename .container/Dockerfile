FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build-stage

ARG TARGETOS 
ARG TARGETARCH
ARG APPVERSION
ARG BUILDNUMBER

WORKDIR /app

COPY . ./


RUN go install github.com/DataDog/orchestrion@v1.5.0 \
    && orchestrion pin \
    && go mod tidy \
    && go mod verify \
    && go mod vendor \
    && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOFLAGS="${GOFLAGS} '-toolexec=orchestrion toolexec'" go build o /app -v ./...


# Deploy the application binary into a lean image
FROM gcr.io/distroless/python3-debian12 AS build-release-stage

WORKDIR /app

COPY --from=build-stage /app /app

COPY .container/appconfig.yaml /app/appconfig.yaml

USER nonroot:nonroot

ENTRYPOINT ["/app/arcane-operator"]