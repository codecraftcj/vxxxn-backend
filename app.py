from flask import Flask, jsonify, request
import os
import requests
import moviepy.editor as mpe
import re
import praw
import boto3
import configparser

#CONFIG PARSER INIT
config = configparser.RawConfigParser()
config.read("config.ini")

# S3 BUCKET CREDENTIALS & DETAILS
S3_REGION = config.get("S3 Bucket Credentials","S3_REGION")
S3_ACCESS_KEY_ID = config.get("S3 Bucket Credentials","S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = config.get("S3 Bucket Credentials","S3_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = config.get("S3 Bucket Credentials","S3_BUCKET_NAME")

app = Flask(__name__)

s3 = boto3.resource(
    service_name='s3',
    region_name=S3_REGION,
    aws_access_key_id=S3_ACCESS_KEY_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY
)

@app.route('/')
def home():
    return jsonify(message="Welcome to the Video Scraper API")

@app.route('/save-to-s3', methods=['POST'])
def save_to_s3():
    data = request.get_json()
    video_file_name = f"{data['output_folder']}/temp_video.mp4"
    with open(video_file_name, 'wb') as video_file:
        video_file.write(requests.get(data['reddit_video_url']).content)
    
    audio_url = re.sub(r"(v.redd.it/\w+/)(\w+)(\.mp4)", r"\1DASH_audio\3", data['reddit_video_url'])
    audio_file_name = f"{data['output_folder']}/temp_audio.mp4"
    with open(audio_file_name, 'wb') as audio_file:
        audio_file.write(requests.get(audio_url).content)

    output_file_name = f"{data['output_name']}.mp4"

    video_clip = mpe.VideoFileClip(video_file_name)
    audio_clip = mpe.AudioFileClip(audio_file_name)
    final_clip = video_clip.set_audio(audio_clip)
    print(f"Saving: {output_file_name}")
    final_clip.write_videofile(f"{data['output_folder']}/{output_file_name}", logger=None)
    s3.Bucket(S3_BUCKET_NAME).upload_file(Filename=output_file_name, Key=output_file_name)
    return jsonify(message="Saved to S3", data=data)

if __name__ == '__main__':
    app.run(debug=True)