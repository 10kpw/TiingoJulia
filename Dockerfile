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

# Install Julia
ENV JULIA_VERSION=1.10.2
RUN wget https://julialang-s3.julialang.org/bin/linux/x64/1.10/julia-${JULIA_VERSION}-linux-x86_64.tar.gz && \
    tar -xvzf julia-${JULIA_VERSION}-linux-x86_64.tar.gz && \
    mv julia-${JULIA_VERSION} /opt/julia && \
    ln -s /opt/julia/bin/julia /usr/local/bin/julia && \
    rm julia-${JULIA_VERSION}-linux-x86_64.tar.gz

# Set environment variable for Julia
ENV PATH="/opt/julia/bin:$PATH"

# Set up default working directory
WORKDIR /workspace