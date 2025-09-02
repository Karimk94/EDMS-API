import requests
import os
import io
import time
import json

def get_captions(image_data, filename):
    api_url = os.getenv("CAPTIONING_API_URL")
    files = {'image_file': (filename, io.BytesIO(image_data), 'application/octet-stream')}
    response = requests.post(api_url, files=files, timeout=300)
    response.raise_for_status()
    return response.json().get('caption', '')

def get_ocr_text(image_data, filename):
    api_url = os.getenv("OCR_API_URL")
    files = {'image_file': (filename, io.BytesIO(image_data), 'application/octet-stream')}
    response = requests.post(api_url, files=files, timeout=300)
    response.raise_for_status()
    return response.json().get('text', '')

def recognize_faces(image_data, filename):
    """Processes a single, full image file for face recognition."""
    api_url = os.getenv("FACE_API_URL")
    files = {'image_file': (filename, io.BytesIO(image_data), 'application/octet-stream')}
    response = requests.post(api_url, files=files, timeout=500)
    response.raise_for_status()
    return response.json().get('faces', [])

def recognize_faces_from_list(base64_faces_list):
    """Processes a list of pre-cropped, base64-encoded faces from a video."""
    base_api_url = os.getenv("FACE_API_URL")
    # Assumes the base URL points to '/analyze_image', so we replace it
    # to target the correct endpoint for pre-cropped faces.
    endpoint = base_api_url.replace('/analyze_image', '/recognize_faces')
    
    payload = {'faces': base64_faces_list}
    response = requests.post(endpoint, json=payload, timeout=500)
    response.raise_for_status()
    return response.json().get('faces', [])

def summarize_video(video_data, filename):
    """Calls the video summarizer API and polls for the result."""
    api_url = os.getenv("VIDEO_SUMMARIZER_API_URL")
    upload_endpoint = f"{api_url}/upload"
    files = {'video': (filename, io.BytesIO(video_data), 'application/octet-stream')}
    
    response = requests.post(upload_endpoint, files=files, data={'language': 'english'}, timeout=300)
    response.raise_for_status()
    task_id = response.json().get('task_id')

    if not task_id:
        raise Exception("Failed to get a task ID from the video summarizer.")

    status_endpoint = f"{api_url}/status/{task_id}"
    while True:
        time.sleep(5)
        status_response = requests.get(status_endpoint, timeout=60)
        status_response.raise_for_status()
        status_data = status_response.json()

        if status_data['status'] == 'complete':
            return status_data['result']
        elif status_data['status'] == 'error':
            raise Exception(f"Video summarization failed: {status_data.get('error')}")

def tokenize_transcript(transcript):
    """Calls the translator/rephraser API to tokenize the transcript."""
    api_url = os.getenv("TRANSLATOR_REPHRASER_API_URL")
    payload = {"text": transcript, "task": "rephrase"}
    response = requests.post(f"{api_url}/generate", json=payload, timeout=300, stream=True)
    response.raise_for_status()
    
    result_text = ""
    for line in response.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            if decoded_line.startswith('data: '):
                data = decoded_line[6:]
                if data.strip() == '[END_OF_STREAM]':
                    break
                result_text += data
    return result_text