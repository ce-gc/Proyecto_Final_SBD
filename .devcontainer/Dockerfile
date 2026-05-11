FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
      pyspark \
      numpy \
      requests \
      pandas \
      pyarrow

WORKDIR /work