FROM apache/spark:3.5.1

USER root

# ============================================================================
# INSTALL SYSTEM DEPENDENCIES (Minimal)
# ============================================================================
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        netcat-openbsd \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# ============================================================================
# DOWNLOAD REQUIRED JARS FOR S3/MinIO SUPPORT
# ============================================================================
# Hadoop AWS: Provides S3A filesystem implementation
# AWS SDK Bundle: Required for S3 API calls
# Versions compatible with Spark 3.5.1 (Hadoop 3.3.4)
RUN cd /opt/spark/jars && \
    curl -fsSL -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o postgresql-42.7.1.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# ============================================================================
# VERIFY JARS ARE DOWNLOADED
# ============================================================================
RUN ls -lh /opt/spark/jars/ | grep -E "hadoop-aws|aws-java-sdk|postgresql" || \
    (echo "❌ Failed to download required JARs" && exit 1)

# ============================================================================
# CREATE DIRECTORIES WITH PROPER PERMISSIONS
# ============================================================================
RUN mkdir -p \
        /opt/spark-apps \
        /opt/spark-data/raw \
        /opt/spark-data/staging \
        /opt/spark-data/production \
        /opt/spark-events \
        /opt/spark/conf \
    && chown -R spark:spark \
        /opt/spark-apps \
        /opt/spark-data \
        /opt/spark-events \
        /opt/spark/conf \
    && chmod -R 755 \
        /opt/spark-apps \
        /opt/spark-data \
        /opt/spark-events \
        /opt/spark/conf

# ============================================================================
# CONFIGURE SPARK DEFAULTS (Optional but recommended)
# ============================================================================
RUN echo "# S3/MinIO Configuration" > /opt/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.path.style.access=true" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.connection.ssl.enabled=false" >> /opt/spark/conf/spark-defaults.conf && \
    chown spark:spark /opt/spark/conf/spark-defaults.conf

# ============================================================================
# SWITCH TO SPARK USER
# ============================================================================
USER spark

WORKDIR /opt/spark

# ============================================================================
# HEALTHCHECK (for Docker Compose)
# ============================================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080 || exit 1