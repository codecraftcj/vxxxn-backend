from flask import Flask, jsonify, request
import os
import requests
import moviepy.editor as mpe
import re
import boto3
import configparser
import json
import threading
import sqlite3
from queue import Queue
from datetime import datetime
import uuid

# CONFIG PARSER INIT
config = configparser.RawConfigParser()
config.read("config.ini")

# S3 BUCKET CREDENTIALS & DETAILS
S3_REGION = config.get("S3 Bucket Credentials", "S3_REGION")
S3_ACCESS_KEY_ID = config.get("S3 Bucket Credentials", "S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = config.get("S3 Bucket Credentials", "S3_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = config.get("S3 Bucket Details", "S3_BUCKET_NAME")

app = Flask(__name__)

# S3 Client
s3 = boto3.resource(
    service_name='s3',
    region_name=S3_REGION,
    aws_access_key_id=S3_ACCESS_KEY_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY
)

# SQLite database setup
DB_NAME = "jobs.db"

def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT
            )
        """)
        conn.commit()

init_db()

# Job queue
job_queue = Queue()

def generate_unique_folder(bucket_name, base_folder="videos"):
    """
    Generate a unique folder name and ensure it does not exist in S3.
    
    :param bucket_name: The name of the S3 bucket.
    :param base_folder: The base folder where the unique folder will be created.
    :return: A unique folder name.
    """
    while True:
        unique_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        folder_name = f"{base_folder}/{unique_id}"

        # Check if folder already exists in S3
        bucket = s3.Bucket(bucket_name)
        existing_objects = list(bucket.objects.filter(Prefix=folder_name))

        if not existing_objects:  # Folder does not exist
            
            return folder_name
        
def process_job():
    while True:
        job = job_queue.get()
        if job is None:
            break

        job_id = job['job_id']
        try:
            data = json.loads(job['data'])
            output_folder = generate_unique_folder(S3_BUCKET_NAME)

            video_file_name = f"temp_video.mp4"
            audio_file_name = f"temp_audio.mp4"
            output_file_name = f"temp_compiled_video.mp4"

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            }
            response = requests.get(data['reddit_post_url'] + ".json", headers=headers)
            metadata = json.loads(response.content)

            # File path to the JSON file
            metadata_file_name = 'metadata.json'

            # Write the dictionary to a JSON file
            with open(metadata_file_name, 'w') as file:
                json.dump(metadata, file, indent=4)
    
            # Extract video URL and download video
            video_url = metadata[0]['data']['children'][0]['data']['secure_media']['reddit_video']['fallback_url']
            with open(video_file_name, 'wb') as video_file:
                video_file.write(requests.get(video_url, headers=headers).content)

            # Extract audio URL and download audio
            audio_url = re.sub(r"(v.redd.it/\w+/)(\w+)(\.mp4)", r"\1DASH_AUDIO_128\3", video_url)
            with open(audio_file_name, 'wb') as audio_file:
                audio_file.write(requests.get(audio_url).content)

            # Combine video and audio
            video_clip = mpe.VideoFileClip(video_file_name)
            audio_clip = mpe.AudioFileClip(audio_file_name)
            final_clip = video_clip.set_audio(audio_clip)
            final_clip.write_videofile(output_file_name, logger=None)
            
            # Upload metadata.json to S3
            s3.Bucket(S3_BUCKET_NAME).upload_file(Filename=metadata_file_name, Key=f"{output_folder}/metadata.json")

            # Upload processed video to S3
            s3.Bucket(S3_BUCKET_NAME).upload_file(Filename=output_file_name, Key=f"{output_folder}/reddit_video.mp4")

            # Cleanup local files
            os.remove(metadata_file_name)
            os.remove(audio_file_name)
            os.remove(video_file_name)
            os.remove(output_file_name)

            update_job_status(job_id, "completed", f"Video and metadata saved in S3 under {output_folder}")
        
        except Exception as e:
            update_job_status(job_id, "failed", str(e))
        
        finally:
            job_queue.task_done()

def update_job_status(job_id, status, message):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE jobs SET status = ?, message = ? WHERE id = ?", (status, message, job_id))
        conn.commit()

# Start a worker thread to process jobs
threading.Thread(target=process_job, daemon=True).start()

@app.route('/')
def home():
    return jsonify(message="Welcome to the Video Scraper API")

@app.route('/save-to-s3', methods=['POST'])
def save_to_s3():
    try:
        data = request.get_json()
        data_str = json.dumps(data)
        
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO jobs (data, status) VALUES (?, ?)", (data_str, "queued"))
            job_id = cursor.lastrowid
            conn.commit()

        # Add job to the queue
        job = {'job_id': job_id, 'data': data_str}
        job_queue.put(job)

        return jsonify(message="Job successfully queued", job_id=job_id), 202
    except Exception as e:
        return jsonify(error=str(e)), 400

@app.route('/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, data, status, message FROM jobs WHERE id = ?", (job_id,))
        job = cursor.fetchone()

    if not job:
        return jsonify(error="Job not found"), 404

    return jsonify(job_id=job[0], data=json.loads(job[1]), status=job[2], message=job[3])

@app.route('/jobs', methods=['GET'])
def get_all_jobs():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, data, status, message FROM jobs")
        jobs = cursor.fetchall()

    jobs_list = [
        {"job_id": job[0], "data": json.loads(job[1]), "status": job[2], "message": job[3]}
        for job in jobs
    ]
    return jsonify(jobs=jobs_list)

@app.route('/jobs/<int:job_id>', methods=['DELETE'])
def delete_job(job_id):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()

    if cursor.rowcount == 0:
        return jsonify(error="Job not found"), 404

    return jsonify(message="Job deleted successfully")

@app.route('/jobs', methods=['DELETE'])
def clear_jobs():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM jobs")
        conn.commit()

    return jsonify(message="All jobs cleared successfully")

if __name__ == '__main__':
    app.run(debug=True)