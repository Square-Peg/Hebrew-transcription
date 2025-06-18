#!/bin/bash
# Startup script for transcription instances

echo "[$(date)] Starting Hebrew transcription instance..."

# Activate virtual environment
source /opt/hebrew-transcription/venv/bin/activate

# Check if required environment variables are set
if [ -z "$S3_BUCKET" ] || [ -z "$S3_KEY" ] || [ -z "$FILENAME" ]; then
    echo "ERROR: Required environment variables not set"
    echo "Need: S3_BUCKET, S3_KEY, FILENAME"
    exit 1
fi

# Log system info
echo "System Information:"
nvidia-smi
echo "Python: $(which python)"
echo "Working directory: $(pwd)"

# Run the transcription
cd /opt/hebrew-transcription
python transcribe.py

# The script should handle shutdown in transcribe.py