import requests
import os

def get_captions(image_path):
    api_url = os.getenv("CAPTIONING_API_URL")
    with open(image_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(api_url, files=files)
        response.raise_for_status()
        return response.json().get('caption', '')

def get_ocr_text(image_path):
    api_url = os.getenv("OCR_API_URL")
    with open(image_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(api_url, files=files)
        response.raise_for_status()
        return response.json().get('text', '')

def recognize_faces(image_path):
    api_url = os.getenv("FACE_API_URL")
    with open(image_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(api_url, files=files)
        response.raise_for_status()
        return response.json().get('faces', [])