FROM ubuntu:22.04

WORKDIR /app

RUN apt-get update && apt-get install -y curl git

RUN git clone --branch "feat/re-naming" https://github.com/radiusxyz/secure-rpc-provider /app/secure-rpc-provider

COPY ./bin/secure-rpc /app/secure-rpc-provider/target/release/secure-rpc

RUN chmod +x /app/secure-rpc-provider/target/release/secure-rpc || true
RUN find /app/secure-rpc-provider/scripts -type f -name "*.sh" -exec chmod +x {} \; || true