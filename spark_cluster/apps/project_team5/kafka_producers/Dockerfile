# Use a lightweight base image
FROM python:3.9-alpine

# Set environment variables
ENV KAFKA_VERSION=2.7.0
ENV SCALA_VERSION=2.13

# Install dependencies
RUN apk add --no-cache bash
RUN apk add --no-cache gcc libc-dev linux-headers musl-dev
RUN pip install kafka-python