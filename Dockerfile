FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    tar \
    curl \
    git \
    build-essential \
    libatomic1 \
    libgmp-dev \
    libmpfr-dev \
    libblas-dev \
    liblapack-dev \
    libopenblas-dev \
    libfftw3-dev \
    libgfortran5 \
    && rm -rf /var/lib/apt/lists/*

# Install Julia with architecture detection
ENV JULIA_VERSION=1.10.9

# Detect architecture and set appropriate Julia download URL
RUN if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ]; then \
        JULIA_ARCH="aarch64"; \
        JULIA_URL="https://julialang-s3.julialang.org/bin/linux/aarch64/1.10/julia-${JULIA_VERSION}-linux-aarch64.tar.gz"; \
    else \
        JULIA_ARCH="x86_64"; \
        JULIA_URL="https://julialang-s3.julialang.org/bin/linux/x64/1.10/julia-${JULIA_VERSION}-linux-x86_64.tar.gz"; \
    fi && \
    wget $JULIA_URL && \
    tar -xvzf julia-${JULIA_VERSION}-linux-${JULIA_ARCH}.tar.gz && \
    mv julia-${JULIA_VERSION} /opt/julia && \
    ln -s /opt/julia/bin/julia /usr/local/bin/julia && \
    rm julia-${JULIA_VERSION}-linux-${JULIA_ARCH}.tar.gz

# Set environment variable for Julia
ENV PATH="/opt/julia/bin:$PATH"

# Set up default working directory
WORKDIR /workspace
