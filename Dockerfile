# Dockerfile for datamanifest
# Uses micromamba for fast conda environment setup

FROM mambaorg/micromamba:latest

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

USER $MAMBA_USER

# Copy conda environment file
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml

# Create conda environment
RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

# Ensure conda environment is activated for subsequent RUN commands
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# Copy package source
COPY --chown=$MAMBA_USER:$MAMBA_USER . /app
WORKDIR /app

# Install package
RUN pip install . --no-deps -vv

# Verify installation
RUN command -v dm

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
CMD ["dm", "--help"]
