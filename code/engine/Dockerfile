# Use an official Python runtime as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy GeoLite databases into the container
COPY GeoLite2-ASN-Test.mmdb /app/
COPY GeoLite2-City.mmdb /app/

# Expose any necessary ports if your engine service listens on any port
EXPOSE 80

# Command to run the engine service
CMD ["python", "engine_main.py"]