FROM apache/spark:3.5.1

USER root

# Install curl and netcat for healthcheck and debugging
RUN apt-get update && \
    apt-get install -y curl netcat-openbsd procps && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /opt/spark-apps /opt/spark-data /opt/spark-events && \
    chmod -R 777 /opt/spark-apps /opt/spark-data /opt/spark-events

# Verify curl installation
RUN curl --version

USER spark
WORKDIR /opt/spark