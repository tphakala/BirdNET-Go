ARG TFLITE_LIB_DIR=/usr/lib

FROM --platform=$BUILDPLATFORM golang:1.23.2-bookworm AS buildenv

# Install zip utility along with other dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    sudo \
    zip \
    npm \
    gcc-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

# Create dev-user for building and devcontainer usage
RUN groupadd --gid 10001 dev-user; \
    useradd --uid 10001 --gid dev-user --shell /bin/bash --create-home dev-user; \
    usermod -aG sudo dev-user; \
    usermod -aG audio dev-user; \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER dev-user

WORKDIR /home/dev-user/src/BirdNET-Go

COPY --chown=dev-user ./Makefile ./
COPY --chown=dev-user ./reset_auth.sh ./

# Download TensorFlow headers
ARG TENSORFLOW_VERSION
RUN make check-tensorflow

# Download and configure precompiled TensorFlow Lite C library
ARG TARGETPLATFORM
ARG TFLITE_LIB_DIR
RUN TFLITE_LIB_ARCH=$(echo ${TARGETPLATFORM} | tr '/' '_').tar.gz \
    make download-tflite

FROM --platform=$BUILDPLATFORM buildenv AS build
WORKDIR /home/dev-user/src/BirdNET-Go

# Compile BirdNET-Go
COPY --chown=dev-user . ./
ARG TARGETPLATFORM
RUN --mount=type=cache,target=/go/pkg/mod,uid=10001,gid=10001 \
    --mount=type=cache,target=/home/dev-user/.cache/go-build,uid=10001,gid=10001 \
    make $(echo ${TARGETPLATFORM} | tr '/' '_')

# Download latest versions of Leaflet, htmx, Alpine.js and Tailwind CSS
RUN make download-assets
RUN make generate-tailwindcss

# Create final image using a multi-platform base image
FROM debian:bookworm-slim

# Install ALSA library and SOX
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libasound2 \
    ffmpeg \
    sox \
    gosu \
    && rm -rf /var/lib/apt/lists/*

ARG TFLITE_LIB_DIR
COPY --from=build ${TFLITE_LIB_DIR}/libtensorflowlite_c.so ${TFLITE_LIB_DIR}
RUN ldconfig

# Include reset_auth tool from build stage
COPY --from=build /home/dev-user/src/BirdNET-Go/reset_auth.sh /usr/bin/
RUN chmod +x /usr/bin/reset_auth.sh

# Create birdnet-user for running the application
RUN groupadd --gid 10002 birdnet-user; \
    useradd --uid 10002 --gid birdnet-user --shell /bin/bash --create-home birdnet-user; \
    usermod -aG audio birdnet-user

# Create config and data directories
RUN mkdir -p  /config /data

VOLUME /config
ENV CONFIG_PATH /config

VOLUME /data
WORKDIR /data

# Make port 8080 available to the world outside this container
EXPOSE 8080

COPY --from=build /home/dev-user/src/BirdNET-Go/bin /usr/bin/

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

CMD ["realtime"]
