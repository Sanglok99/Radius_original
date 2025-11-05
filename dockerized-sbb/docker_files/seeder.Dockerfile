FROM ubuntu:22.04

WORKDIR /app

RUN apt-get update && apt-get install -y curl git

RUN git clone https://github.com/radiusxyz/seeder /app/seeder

COPY ./bin/seeder /app/seeder/target/release/seeder

RUN chmod +x /app/seeder/target/release/seeder || true
RUN find /app/seeder/scripts -type f -name "*.sh" -exec chmod +x {} \; || true