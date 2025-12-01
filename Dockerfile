# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies needed for pdfplumber and others
# (Ghostscript/ImageMagick usually not needed for pdfplumber unless using specific features, 
# but libgl1 might be needed if OpenCV were used - keeping it minimal for now)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
# We copy 'src' and 'data' if needed, but mainly 'src' for the app code
COPY src ./src
# Create temp directories
RUN mkdir -p temp_uploads

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable
ENV PYTHONPATH=/app

# Run app.py when the container launches
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
