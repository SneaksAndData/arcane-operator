FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
ARG TARGETARCH

WORKDIR /app

# Copy csproj and restore as distinct layers
COPY src/*.csproj ./
RUN dotnet restore

# Copy everything else and build
COPY src/. ./
RUN dotnet publish "Arcane.Operator.csproj" -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0-bookworm-slim
ARG TRACER_VERSION="2.49.0"
ARG TARGETARCH

RUN apt-get update -y && apt-get install -y curl jq

# Download and install the Datadog Tracer
RUN mkdir -p /opt/datadog \
    && echo $TARGETARCH \
    && mkdir -p /var/log/datadog \
    && curl -LO https://github.com/DataDog/dd-trace-dotnet/releases/download/v${TRACER_VERSION}/datadog-dotnet-apm_${TRACER_VERSION}_${TARGETARCH}.deb \
    && dpkg -i ./datadog-dotnet-apm_${TRACER_VERSION}_${TARGETARCH}.deb \
    && rm ./datadog-dotnet-apm_${TRACER_VERSION}_${TARGETARCH}.deb 
    

WORKDIR /app
COPY --from=build-env /app/out .

USER app

ENTRYPOINT "dotnet" "Arcane.Operator.dll"
