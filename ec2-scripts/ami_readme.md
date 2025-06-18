# Hebrew Transcription AMI Documentation

## Version: 1.0
## Created: December 2024

### Pre-installed Components:
- Ubuntu 22.04 LTS
- NVIDIA Driver 535.183.01
- CUDA 12.1
- Python 3.10 with virtual environment at /opt/hebrew-transcription/venv
- Hebrew Whisper model (ivrit-ai/whisper-large-v3-ct2)
- Translation model (Helsinki-NLP/opus-mt-tc-big-he-en)
- Speaker diarization (Resemblyzer)

### Instance Requirements:
- Instance Type: g5.xlarge (or any GPU instance)
- Storage: Minimum 100GB gp3
- IAM Role: Must have S3 read/write permissions

### Usage:
The instance expects these environment variables in UserData:
- S3_BUCKET: The S3 bucket containing audio files
- S3_KEY: The full S3 key to the audio file
- FILENAME: The filename (for logging)

### Main Script:
/opt/hebrew-transcription/transcribe.py

### Testing:
- /home/ubuntu/test_pipeline.py - Test with local file
- /home/ubuntu/update_diarization.py - Update diarization algorithm
- /home/ubuntu/business_meeting_diarization.py - Business meeting optimization

### Directory Structure:
/opt/hebrew-transcription/
├── transcribe.py          # Main transcription script
├── venv/                  # Python virtual environment
├── models/                # Pre-downloaded models
├── startup.sh            # Instance startup script
└── AMI_README.md         # This file