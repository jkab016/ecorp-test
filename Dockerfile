# Use an official Python image
FROM python:3.11.8-slim

# Install Java and upgrade system packages
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
# Set environment variables to ensure Python does not create .pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# Set environment variable to ensure Python outputs everything to the console
ENV PYTHONUNBUFFERED 1

# Create app directory
WORKDIR /app

# Copy project files
COPY . .

# Generate requirements and install
RUN pip install --no-cache-dir pipreqs \
 && pipreqs . --force \
 && pip install --no-cache-dir -r requirements.txt

# Entrypoint for running the pipeline

ENTRYPOINT ["python", "main.py"]