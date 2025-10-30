#!/bin/bash
# Install Python dependencies
pip install -r requirements.txt

# Create necessary directories
mkdir -p data
mkdir -p static
mkdir -p templates

# Start the Flask application with Gunicorn
gunicorn --bind 0.0.0.0:8000 --timeout 600 --workers 2 app:app