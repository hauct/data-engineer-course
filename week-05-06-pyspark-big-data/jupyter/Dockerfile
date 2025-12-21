# ==============================================================================
# SIMPLIFIED: Jupyter Lab with PySpark - Based on apache/spark
# ==============================================================================
FROM apache/spark:3.5.1

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
        python3-pip \
        python3-dev \
        build-essential \
        vim \
        nano \
        curl \
        wget \
        git \
        bash-completion && \
    rm -rf /var/lib/apt/lists/* && \
    echo ". /usr/share/bash-completion/bash_completion" >> /etc/bash.bashrc

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Download additional JARs (Delta Lake, PostgreSQL, AWS)
RUN cd ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# Create directories
RUN mkdir -p /home/spark/.local/share/jupyter/runtime && \
    mkdir -p /opt/spark-notebooks && \
    mkdir -p /opt/spark-data && \
    mkdir -p /opt/spark-apps && \
    mkdir -p /opt/spark-scripts && \
    mkdir -p /opt/spark-events && \
    mkdir -p /opt/shared && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark-notebooks && \
    chown -R spark:spark /opt/spark-data && \
    chown -R spark:spark /opt/spark-apps && \
    chown -R spark:spark /opt/spark-scripts && \
    chown -R spark:spark /opt/spark-events && \
    chown -R spark:spark /opt/shared

USER spark
ENV SHELL=/bin/bash

WORKDIR /opt/spark-notebooks
EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=dataengineer"]