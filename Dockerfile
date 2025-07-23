FROM python:3.11-slim

# Set the working directory
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Copy the requirements file into the container
COPY requirements.txt .
# Install system dependencies
RUN pip install --upgrade pip && \
    apt-get update && \
    pip install --no-cache-dir -r requirements.txt
# Copy the rest of the application code into the container
COPY . .