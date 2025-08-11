# Use an official Python image
FROM python:3.12-slim

# Install dependencies for Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    openjdk-17-jre-headless \
    git \
    && rm -rf /var/lib/apt/lists/*
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Create app directory
WORKDIR /app

#copy project files
COPY . .

# Generate requirements and install
RUN pip install --no-cache-dir pipreqs \
 && pipreqs . --force \
 && pip install --no-cache-dir -r requirements.txt

# Entrypoint for running the pipeline
ENTRYPOINT ["python", "main.py"]