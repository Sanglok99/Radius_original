FROM ubuntu:22.04

WORKDIR /app

RUN apt-get update && apt-get install -y curl git

RUN git clone https://github.com/radiusxyz/distributed_key_generator /app/distributed_key_generator

COPY ./bin/key-generator /app/distributed_key_generator/target/release/key-generator

RUN chmod +x /app/distributed_key_generator/target/release/key-generator || true
RUN find /app/distributed_key_generator/scripts -type f -name "*.sh" -exec chmod +x {} \; || true