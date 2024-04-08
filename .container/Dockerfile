FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
ARG TARGETARCH
ARG INSTALL_DD_TRACER="true"

WORKDIR /app

# Copy csproj and restore as distinct layers
COPY src/*.csproj ./
RUN dotnet_arch=$(test "$TARGETARCH" = "amd64" && echo "linux-x64" else echo "linux-$TARGETARCH") && \
    echo "dotnet RID: $dotnet_arch" && \
    dotnet restore --architecture "$dotnet_arch"

# Copy everything else and build
COPY src/. ./
RUN dotnet_arch=$(test "$TARGETARCH" = "amd64" && echo "linux-x64" else echo "linux-$TARGETARCH") && \
    echo "dotnet RID: $dotnet_arch" && \
    dotnet publish "Arcane.Operator.csproj" -c Release -o out --architecture "$dotnet_arch"

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0-bookworm-slim
ARG TRACER_VERSION="2.49.0"
ARG INSTALL_DD_TRACER="true"
ARG TARGETARCH

RUN apt-get update -y && apt-get install -y curl jq

# Download and install the Datadog Tracer
RUN if [ -z "$INSTALL_DATADOG_TRACER" ]; then \
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
