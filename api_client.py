import requests
import os
import io

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
    api_url = os.getenv("FACE_API_URL")
    files = {'image_file': (filename, io.BytesIO(image_data), 'application/octet-stream')}
    response = requests.post(api_url, files=files, timeout=500)
    response.raise_for_status()
    return response.json().get('faces', [])
