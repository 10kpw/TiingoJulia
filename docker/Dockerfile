FROM ubuntu:24.04

# Install dependencies
RUN apt-get update && apt-get install -y \
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

# Install juliaup and Julia
ENV JULIA_VERSION=1.12.1
ENV JULIAUP_INSTALL_DIR=/opt/juliaup
ENV PATH="${JULIAUP_INSTALL_DIR}/bin:$PATH"

RUN curl -fsSL https://install.julialang.org | sh -s -- --yes --default-channel ${JULIA_VERSION} && \
    ln -s /root/.juliaup/bin/julia /usr/local/bin/julia && \
    ln -s /root/.juliaup/bin/juliaup /usr/local/bin/juliaup

# Set up default working directory
WORKDIR /workspace
