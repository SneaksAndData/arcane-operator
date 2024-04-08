# The `platform` argument here is required, since dotnet-sdk crashes with segmentation fault 
# in case of arm64 builds, see https://github.com/dotnet/dotnet-docker/issues/4225 for details
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env

ARG INSTALL_DD_TRACER="true"
ARG TRACER_VERSION="2.49.0"
ARG TARGETARCH

WORKDIR /app

# Copy csproj and restore as distinct layers
COPY src/*.csproj ./
RUN dotnet_arch=$(test "$TARGETARCH" = "amd64" && echo "x64" || echo "$TARGETARCH") && \
    dotnet restore --runtime "linux-$dotnet_arch"

# Copy everything else and build
COPY src/. ./
RUN dotnet_arch=$(test "$TARGETARCH" = "amd64" && echo "x64" || echo "$TARGETARCH") && \
    dotnet publish "Arcane.Operator.csproj" -c Release -o out --runtime "linux-$dotnet_arch"

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0-bookworm-slim

ARG TRACER_VERSION="2.49.0"
ARG INSTALL_DD_TRACER="true"
ARG TARGETARCH

RUN apt-get update -y && apt-get install -y curl jq

# Download and install the Datadog Tracer
RUN if [ -z "$INSTALL_DD_TRACER" ]; then \
      echo "Datadog tracer installation skipped"; \
    else \
        mkdir -p /opt/datadog \
        && echo $TARGETARCH \
        && mkdir -p /var/log/datadog \
        && curl -LO https://github.com/DataDog/dd-trace-dotnet/releases/download/v${TRACER_VERSION}/datadog-dotnet-apm_${TRACER_VERSION}_${TARGETARCH}.deb \
        && dpkg -i ./datadog-dotnet-apm_${TRACER_VERSION}_${TARGETARCH}.deb \
        && rm ./datadog-dotnet-apm_${TRACER_VERSION}_${TARGETARCH}.deb ; \
    fi;
    
WORKDIR /app
COPY --from=build-env /app/out .

USER app

ENTRYPOINT "dotnet" "Arcane.Operator.dll"
