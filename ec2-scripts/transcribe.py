#!/usr/bin/env python3
"""
Hebrew Audio Transcription Pipeline
Processes audio files from S3, transcribes Hebrew, diarizes speakers, translates to English
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime
import boto3
import torch
import numpy as np
from faster_whisper import WhisperModel
from transformers import MarianMTModel, MarianTokenizer
from resemblyzer import VoiceEncoder, preprocess_wav
from sklearn.cluster import AgglomerativeClustering, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import librosa
import soundfile as sf
from tqdm import tqdm

# Configuration from environment
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_KEY = os.environ.get('S3_KEY')
FILENAME = os.environ.get('FILENAME', 'audio.mp3')

# AWS clients
s3 = boto3.client('s3')

def log(message):
    """Log with timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def download_from_s3():
    """Download audio file from S3"""
    log(f"Downloading {S3_KEY} from {S3_BUCKET}")
    local_path = f"/tmp/{FILENAME}"
    s3.download_file(S3_BUCKET, S3_KEY, local_path)
    return local_path

def convert_to_wav(input_path):
    """Convert audio to 16kHz mono WAV"""
    log("Converting to 16kHz mono WAV...")
    wav_path = input_path.replace('.mp3', '.wav').replace('.m4a', '.wav')
    
    cmd = [
        'ffmpeg', '-i', input_path,
        '-ar', '16000',  # 16kHz
        '-ac', '1',      # mono
        '-y',            # overwrite
        wav_path
    ]
    
    subprocess.run(cmd, check=True, capture_output=True)
    return wav_path

def transcribe_hebrew(wav_path):
    """Transcribe Hebrew audio using Whisper"""
    log("Loading Whisper model...")
    model = WhisperModel("ivrit-ai/whisper-large-v3-ct2", device="cuda", compute_type="float16")
    
    log("Transcribing Hebrew audio...")
    segments, info = model.transcribe(
        wav_path,
        language="he",
        beam_size=5,
        best_of=5,
        temperature=0,
        word_timestamps=True,
        condition_on_previous_text=True
    )
    
    # Convert generator to list
    segments_list = []
    for segment in tqdm(segments, desc="Processing segments"):
        segments_list.append({
            "start": segment.start,
            "end": segment.end,
            "text": segment.text,
            "words": [
                {
                    "start": word.start,
                    "end": word.end,
                    "word": word.word,
                    "probability": word.probability
                } for word in segment.words
            ] if hasattr(segment, 'words') else []
        })
    
    log(f"Transcription complete. Found {len(segments_list)} segments.")
    return segments_list, info

def diarize_speakers(wav_path, segments):
    """Perform speaker diarization optimized for business meetings (2-4 participants)"""
    log("Loading audio for speaker diarization...")
    
    # Load audio
    audio, sr = librosa.load(wav_path, sr=16000)
    
    log("Extracting speaker embeddings...")
    encoder = VoiceEncoder()
    
    embeddings = []
    valid_segments = []
    segment_indices = []
    
    # For business meetings, we want more stable embeddings
    # Use longer minimum duration for reliable speaker identification
    for idx, segment in enumerate(tqdm(segments, desc="Extracting embeddings")):
        start_sample = int(segment["start"] * sr)
        end_sample = int(segment["end"] * sr)
        duration = segment["end"] - segment["start"]
        
        # Skip very short segments (less than 1.0 seconds)
        if duration < 1.0:
            continue
            
        audio_segment = audio[start_sample:end_sample]
        
        try:
            # For segments 2-5 seconds, use the whole segment
            if duration <= 5.0:
                wav_preprocessed = preprocess_wav(audio_segment)
                embed = encoder.embed_utterance(wav_preprocessed)
                embeddings.append(embed)
                valid_segments.append(segment)
                segment_indices.append(idx)
            else:
                # For longer segments, extract multiple embeddings and average
                # This helps with consistent speaker identification
                window_size = int(3.0 * sr)  # 3 second windows
                hop_size = int(1.5 * sr)     # 1.5 second hop
                
                segment_embeddings = []
                for i in range(0, len(audio_segment) - window_size, hop_size):
                    window = audio_segment[i:i + window_size]
                    wav_preprocessed = preprocess_wav(window)
                    embed = encoder.embed_utterance(wav_preprocessed)
                    segment_embeddings.append(embed)
                
                if segment_embeddings:
                    # Use the average embedding for this segment
                    avg_embedding = np.mean(segment_embeddings, axis=0)
                    embeddings.append(avg_embedding)
                    valid_segments.append(segment)
                    segment_indices.append(idx)
                
        except Exception as e:
            log(f"Warning: Could not process segment {segment['start']}-{segment['end']}: {e}")
    
    if len(embeddings) < 2:
        log("Not enough valid segments for speaker diarization")
        for segment in segments:
            segment["speaker"] = "SPEAKER_1"
        return segments
    
    # Cluster embeddings
    log(f"Clustering {len(embeddings)} embeddings...")
    X = np.array(embeddings)
    
    # For business meetings, we expect 2-4 speakers typically
    # Use more conservative clustering
    from sklearn.cluster import AgglomerativeClustering
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler
    
    # Normalize embeddings
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Try different numbers of speakers (2-5 for business meetings)
    best_score = -1
    best_n_speakers = 2
    best_labels = None
    
    # Calculate pairwise distances to estimate number of speakers
    from sklearn.metrics.pairwise import cosine_distances
    distances = cosine_distances(X)
    
    # Use distance distribution to guide speaker count
    # In meetings, speakers are more distinct
    distance_threshold = np.percentile(distances[distances > 0], 30)
    
    for n_speakers in range(2, min(6, len(embeddings))):
        clustering = AgglomerativeClustering(
            n_clusters=n_speakers, 
            linkage='average',  # Average linkage works well for distinct speakers
            metric='cosine'
        )
        labels = clustering.fit_predict(X)
        
        # Calculate silhouette score
        if n_speakers < len(X):
            score = silhouette_score(X, labels, metric='cosine')
            
            # For business meetings, we prefer fewer speakers unless the score improves significantly
            # This prevents over-segmentation
            if n_speakers > 2:
                # Require significant improvement to add more speakers
                score_improvement_threshold = 0.05
                if score > best_score + score_improvement_threshold:
                    best_score = score
                    best_n_speakers = n_speakers
                    best_labels = labels
            else:
                if score > best_score:
                    best_score = score
                    best_n_speakers = n_speakers
                    best_labels = labels
    
    log(f"Identified {best_n_speakers} speakers (silhouette score: {best_score:.3f})")
    
    # Assign labels to valid segments
    speaker_segments = {}
    for segment, label, idx in zip(valid_segments, best_labels, segment_indices):
        if label not in speaker_segments:
            speaker_segments[label] = []
        speaker_segments[label].append({
            'segment': segment,
            'idx': idx,
            'start': segment['start'],
            'end': segment['end']
        })
    
    # Analyze speaking patterns to better identify speakers
    speaker_stats = {}
    for label, segments_data in speaker_segments.items():
        total_duration = sum(s['segment']['end'] - s['segment']['start'] for s in segments_data)
        avg_duration = total_duration / len(segments_data)
        speaker_stats[label] = {
            'total_duration': total_duration,
            'avg_duration': avg_duration,
            'segment_count': len(segments_data),
            'first_appearance': min(s['start'] for s in segments_data)
        }
    
    # Assign speaker IDs based on first appearance (chronological order)
    speaker_order = sorted(speaker_stats.keys(), key=lambda x: speaker_stats[x]['first_appearance'])
    speaker_mapping = {old_id: f"SPEAKER_{i+1}" for i, old_id in enumerate(speaker_order)}
    
    # Create index to speaker mapping
    idx_to_speaker = {}
    for label, segments_data in speaker_segments.items():
        for seg_data in segments_data:
            idx_to_speaker[seg_data['idx']] = speaker_mapping[label]
    
    # Assign speakers to all segments
    for idx, segment in enumerate(segments):
        if idx in idx_to_speaker:
            segment["speaker"] = idx_to_speaker[idx]
        else:
            # For unprocessed segments, use nearest neighbor approach
            min_dist = float('inf')
            nearest_speaker = "SPEAKER_1"
            seg_mid = (segment["start"] + segment["end"]) / 2
            
            for processed_idx, speaker in idx_to_speaker.items():
                processed_seg = segments[processed_idx]
                processed_mid = (processed_seg["start"] + processed_seg["end"]) / 2
                dist = abs(seg_mid - processed_mid)
                
                # Give preference to the previous speaker if the gap is small
                if idx > 0 and dist < 2.0:  # Within 2 seconds
                    if segments[idx-1].get("speaker"):
                        nearest_speaker = segments[idx-1]["speaker"]
                        break
                
                if dist < min_dist:
                    min_dist = dist
                    nearest_speaker = speaker
            
            segment["speaker"] = nearest_speaker
    
    # Post-processing: Smooth out speaker assignments
    # In business meetings, rapid speaker changes are less common
    window_size = 3
    for i in range(len(segments)):
        if i < window_size or i >= len(segments) - window_size:
            continue
        
        # Check if this segment is an outlier
        window_speakers = []
        for j in range(i - window_size, i + window_size + 1):
            if j != i:
                window_speakers.append(segments[j]["speaker"])
        
        # If all surrounding segments have the same speaker, adopt it
        if len(set(window_speakers)) == 1 and segments[i]["speaker"] != window_speakers[0]:
            # Only change if the segment is short (likely misclassified)
            if segments[i]["end"] - segments[i]["start"] < 3.0:
                segments[i]["speaker"] = window_speakers[0]
    
    # Final merge of consecutive segments with same speaker
    merged = []
    current = None
    
    for segment in segments:
        if current is None:
            current = segment.copy()
        elif (current["speaker"] == segment["speaker"] and 
              segment["start"] - current["end"] < 0.5):
            # Merge with current
            current["end"] = segment["end"]
            current["text"] = current["text"].rstrip() + " " + segment["text"].lstrip()
            if "translation" in current and "translation" in segment:
                current["translation"] = current["translation"].rstrip() + " " + segment["translation"].lstrip()
        else:
            # Save current and start new
            merged.append(current)
            current = segment.copy()
    
    if current:
        merged.append(current)
    
    segments[:] = merged
    
    # Log final speaker distribution
    final_speakers = {}
    for segment in segments:
        speaker = segment["speaker"]
        if speaker not in final_speakers:
            final_speakers[speaker] = {
                'count': 0,
                'duration': 0
            }
        final_speakers[speaker]['count'] += 1
        final_speakers[speaker]['duration'] += segment["end"] - segment["start"]
    
    log("Final speaker distribution:")
    for speaker in sorted(final_speakers.keys()):
        stats = final_speakers[speaker]
        log(f"  {speaker}: {stats['count']} segments, {stats['duration']:.1f}s total speaking time")
    
    return segments

def translate_segments(segments):
    """Translate Hebrew segments to English"""
    log("Loading translation model...")
    tokenizer = MarianTokenizer.from_pretrained("/opt/hebrew-transcription/models/opus-mt-he-en")
    model = MarianMTModel.from_pretrained("/opt/hebrew-transcription/models/opus-mt-he-en")
    
    if torch.cuda.is_available():
        model = model.cuda()
    
    log("Translating segments to English...")
    
    for segment in tqdm(segments, desc="Translating"):
        hebrew_text = segment["text"].strip()
        
        if not hebrew_text:
            segment["translation"] = ""
            continue
        
        # Tokenize and translate
        inputs = tokenizer(hebrew_text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        
        if torch.cuda.is_available():
            inputs = {k: v.cuda() for k, v in inputs.items()}
        
        with torch.no_grad():
            translated = model.generate(**inputs, max_length=512, num_beams=4, early_stopping=True)
        
        translation = tokenizer.decode(translated[0], skip_special_tokens=True)
        segment["translation"] = translation
    
    return segments

def group_segments_by_speaker(segments):
    """Group consecutive segments by the same speaker with stricter criteria"""
    if not segments:
        return []
    
    grouped = []
    current_group = {
        "speaker": segments[0]["speaker"],
        "start": segments[0]["start"],
        "end": segments[0]["end"],
        "text": segments[0]["text"],
        "translation": segments[0].get("translation", "")
    }
    
    for segment in segments[1:]:
        # Only merge if:
        # 1. Same speaker
        # 2. Gap is less than 0.3 seconds (stricter)
        # 3. Not switching between interviewer and interviewee
        same_speaker = segment["speaker"] == current_group["speaker"]
        small_gap = (segment["start"] - current_group["end"]) < 0.3
        
        if same_speaker and small_gap:
            # Extend current group
            current_group["end"] = segment["end"]
            current_group["text"] += " " + segment["text"]
            current_group["translation"] += " " + segment.get("translation", "")
        else:
            # Start new group
            grouped.append(current_group)
            current_group = {
                "speaker": segment["speaker"],
                "start": segment["start"],
                "end": segment["end"],
                "text": segment["text"],
                "translation": segment.get("translation", "")
            }
    
    grouped.append(current_group)
    return grouped

def format_time(seconds):
    """Format seconds to HH:MM:SS"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def save_results(segments, grouped_segments, info):
    """Save results to S3"""
    log("Preparing results...")
    
    # Create JSON output
    output = {
        "metadata": {
            "filename": FILENAME,
            "duration": info.duration if hasattr(info, 'duration') else None,
            "language": "he",
            "processing_time": time.time() - start_time,
            "num_speakers": len(set(s["speaker"] for s in segments)),
            "num_segments": len(segments)
        },
        "segments": segments,
        "grouped_segments": grouped_segments
    }
    
    # Save JSON
    json_path = f"/tmp/{FILENAME.replace('.mp3', '').replace('.m4a', '')}_transcript.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    # Create human-readable transcript
    transcript_lines = []
    transcript_lines.append(f"Hebrew Audio Transcription")
    transcript_lines.append(f"File: {FILENAME}")
    transcript_lines.append(f"Duration: {format_time(info.duration) if hasattr(info, 'duration') else 'Unknown'}")
    transcript_lines.append(f"Speakers: {output['metadata']['num_speakers']}")
    transcript_lines.append("=" * 80)
    transcript_lines.append("")
    
    for segment in grouped_segments:
        start_time_str = format_time(segment["start"])
        end_time_str = format_time(segment["end"])
        transcript_lines.append(f"[{start_time_str} - {end_time_str}] {segment['speaker']}")
        transcript_lines.append(f"Hebrew: {segment['text']}")
        transcript_lines.append(f"English: {segment['translation']}")
        transcript_lines.append("")
    
    txt_path = f"/tmp/{FILENAME.replace('.mp3', '').replace('.m4a', '')}_transcript.txt"
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(transcript_lines))
    
    # Upload to S3
    output_prefix = f"outputs/{FILENAME.replace('.mp3', '').replace('.m4a', '')}"
    
    log("Uploading results to S3...")
    s3.upload_file(json_path, S3_BUCKET, f"{output_prefix}_transcript.json")
    s3.upload_file(txt_path, S3_BUCKET, f"{output_prefix}_transcript.txt")
    
    # Move original file to done/
    log("Moving original file to done/")
    copy_source = {'Bucket': S3_BUCKET, 'Key': S3_KEY}
    s3.copy_object(CopySource=copy_source, Bucket=S3_BUCKET, Key=S3_KEY.replace('raw/', 'raw/done/'))
    s3.delete_object(Bucket=S3_BUCKET, Key=S3_KEY)
    
    return {
        "json_key": f"{output_prefix}_transcript.json",
        "txt_key": f"{output_prefix}_transcript.txt"
    }

def main():
    """Main processing pipeline"""
    global start_time
    start_time = time.time()
    
    try:
        # Download audio
        audio_path = download_from_s3()
        
        # Convert to WAV
        wav_path = convert_to_wav(audio_path)
        
        # Transcribe Hebrew
        segments, info = transcribe_hebrew(wav_path)
        
        # Diarize speakers
        segments = diarize_speakers(wav_path, segments)
        
        # Translate to English
        segments = translate_segments(segments)
        
        # Group by speaker
        grouped_segments = group_segments_by_speaker(segments)
        
        # Save results
        result_keys = save_results(segments, grouped_segments, info)
        
        log(f"Processing complete! Total time: {time.time() - start_time:.2f} seconds")
        log(f"Results saved to: {result_keys}")
        
    except Exception as e:
        log(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Move file to error folder
        try:
            copy_source = {'Bucket': S3_BUCKET, 'Key': S3_KEY}
            error_key = S3_KEY.replace('raw/', 'error/')
            s3.copy_object(CopySource=copy_source, Bucket=S3_BUCKET, Key=error_key)
            s3.delete_object(Bucket=S3_BUCKET, Key=S3_KEY)
            log(f"Moved file to error folder: {error_key}")
        except:
            pass
        
        sys.exit(1)

if __name__ == "__main__":
    main()