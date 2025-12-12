# File: week-01-02-sql-python/jupyter/Dockerfile

FROM jupyter/scipy-notebook:latest

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    vim \
    && rm -rf /var/lib/apt/lists/*

USER jovyan

# Copy requirements
COPY requirements.txt /tmp/

# Install Python packages
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install Jupyter extensions
RUN jupyter labextension install @jupyter-widgets/jupyterlab-manager

# Set working directory
WORKDIR /home/jovyan/work

# Expose port
EXPOSE 8888

CMD ["start-notebook.sh", "--NotebookApp.token='dataengineer'"]