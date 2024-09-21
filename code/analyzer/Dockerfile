# Use an official Python runtime as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any required packages
RUN pip install --no-cache-dir -r requirements.txt

# Specify the command to run the analyzer
CMD ["python", "pca_analysis.py"]
