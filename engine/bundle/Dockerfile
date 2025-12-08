# Spt Bundle Docker Image
# 
# This Dockerfile packages the pre-built Spt bundle into a Docker image.
# It expects the bundle to be already built via './gradlew build' or 'make build'.
#
# Build context should be the bundle directory after running the build.

# Allow overriding the base image (default pins Eclipse Temurin JRE 21 LTS)
# Pinned to digest for reproducibility - update via Dependabot/Renovate
# To find latest digest: docker buildx imagetools inspect eclipse-temurin:21-jre-jammy
ARG BASE_IMAGE=eclipse-temurin:21.0.9_10-jre-jammy@sha256:2843f155a9fe5aab6a73a71a9f65c38143e8e929366a1a7787f07c2a89c26887
ARG SPT_VERSION=0.0.0-dev
ARG SPT_RELEASE_VERSION=${SPT_VERSION}
FROM ${BASE_IMAGE}

# Re-declare ARGs after FROM to make them available
ARG SPT_VERSION
ARG SPT_RELEASE_VERSION

# OCI Image Spec labels
# These provide standard metadata for container registries
# See: https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.title="Dell Storage Performance Tool" \
      org.opencontainers.image.description="High-performance S3-compatible storage benchmarking tool" \
      org.opencontainers.image.version="${SPT_VERSION}" \
      org.opencontainers.image.vendor="Dell Technologies" \
      org.opencontainers.image.url="https://github.com/dell/storage-performance-tool" \
      org.opencontainers.image.source="https://github.com/dell/storage-performance-tool" \
      org.opencontainers.image.licenses="MIT" \
      maintainer="spt-dev@dell.com"

# Legacy version label for backwards compatibility with version sync checks
LABEL version="${SPT_VERSION}"

# Install runtime dependencies
# Note: Temurin images already include the JRE, so install only basic tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Create spt user and directories
# Note: We set the home directory to /home/spt (not /opt/spt)
RUN useradd --home-dir /home/spt --create-home --shell /bin/bash spt && \
    mkdir -p /opt/spt && \
    mkdir -p /home/spt/.spt

# Copy the pre-built bundle artifacts
# These are created by running './gradlew :bundle:build'
COPY --chown=spt:spt build/dist/spt.jar /opt/spt/
COPY --chown=spt:spt build/dist/ext /opt/spt/ext/
COPY --chown=spt:spt docker/entrypoint.sh /opt/spt/

# Copy example scenarios from the distribution
# These provide ready-to-use test scenarios for common use cases
COPY --chown=spt:spt build/dist/scenarios /opt/spt/scenarios

# Set up the extensions directory structure
# Spt expects extensions in ~/.spt/<version>/ext/
RUN mkdir -p /home/spt/.spt/${SPT_RELEASE_VERSION}/ext && \
    cd /home/spt/.spt/${SPT_RELEASE_VERSION}/ext && \
    for ext in /opt/spt/ext/*.jar; do \
        ln -s "$ext" "$(basename "$ext")"; \
    done && \
    chown -R spt:spt /home/spt/.spt

# Make entrypoint executable
RUN chmod +x /opt/spt/entrypoint.sh

# Create a workspace directory for user scenarios and data
# This is a convenient mount point for volumes
RUN mkdir -p /workspace && \
    chown spt:spt /workspace

# Switch to spt user
USER spt

# Set working directory
WORKDIR /opt/spt

# Environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV SPT_VERSION=${SPT_RELEASE_VERSION}
ENV SPT_SCENARIOS=/opt/spt/scenarios

# Expose typical Spt ports
# 9999 - default Spt REST API port
EXPOSE 9999

# Set the entrypoint
ENTRYPOINT ["/opt/spt/entrypoint.sh"]

# Default command (can be overridden)
CMD ["--version"]
