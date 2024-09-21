# Use an official Python runtime as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy over the environment script
COPY environment.sh /app/
RUN chmod +x environment.sh

# Expose any necessary ports if needed
EXPOSE 80

# Start packet sniffer
CMD ["bash", "./environment.sh", "&&", "python", "packet_sniffer.py"]