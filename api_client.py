import requests
import os
import io
import time

def data_chunk_generator(data, chunk_size=262144):
    """Yields chunks of binary data from a BytesIO object."""
    with io.BytesIO(data) as data_stream:
        while True:
            chunk = data_stream.read(chunk_size)
            if not chunk:
                break
            yield chunk

def get_captions(image_data, filename):
    """Calls the image captioning service using a stream."""
    api_url = os.getenv("CAPTIONING_API_URL")
    stream_api_url = f"{api_url.rstrip('/')}/process_image_stream"
    
    headers = {'Content-Type': 'application/octet-stream', 'X-Filename': filename}
    response = requests.post(stream_api_url, data=data_chunk_generator(image_data), headers=headers, timeout=300)
    
    response.raise_for_status()
    return response.json()

def get_ocr_text(image_data, filename):
    """Calls the OCR service for images using a stream."""
    api_url = f"{os.getenv('OCR_API_URL')}/translate_image_stream"
    headers = {'Content-Type': 'application/octet-stream', 'X-Filename': filename}
    response = requests.post(api_url, data=data_chunk_generator(image_data), headers=headers, timeout=300)
    response.raise_for_status()
    return response.json().get('text', '')

def get_ocr_text_from_pdf(pdf_data, filename):
    """Calls the PDF processing endpoint in the OCR service using a stream."""
    base_api_url = os.getenv("OCR_API_URL")
    pdf_api_url = f"{base_api_url}/process_pdf_stream"
    
    headers = {'Content-Type': 'application/octet-stream', 'X-Filename': filename}
    response = requests.post(pdf_api_url, data=data_chunk_generator(pdf_data), headers=headers, timeout=300)
    response.raise_for_status()
    return response.json().get('text', '')

def recognize_faces(image_data, filename):
    """Processes a single, full image file for face recognition using a stream."""
    api_url = os.getenv("FACE_API_URL")
    stream_api_url = f"{api_url.rstrip('/')}/api/analyze_image_stream"

    headers = {'Content-Type': 'application/octet-stream', 'X-Filename': filename}
    response = requests.post(stream_api_url, data=data_chunk_generator(image_data), headers=headers, timeout=500)
    response.raise_for_status()
    return response.json().get('faces', [])

def recognize_faces_from_list(base64_faces_list):
    """Processes a list of pre-cropped, base64-encoded faces from a video."""
    base_api_url = os.getenv("FACE_API_URL")
    endpoint = f"{base_api_url.rstrip('/')}/recognize_faces"
    
    payload = {'faces': base64_faces_list}
    response = requests.post(endpoint, json=payload, timeout=500)
    response.raise_for_status()
    return response.json().get('faces', [])

def summarize_video(video_data, filename):
    """Calls the video summarizer API using a streaming upload."""
    api_url = os.getenv("VIDEO_SUMMARIZER_API_URL")
    upload_endpoint = f"{api_url}/upload_stream"
    
    params = {'language': 'english'}
    headers = {'Content-Type': 'application/octet-stream', 'X-Filename': filename}
    
    response = requests.post(upload_endpoint, data=data_chunk_generator(video_data), headers=headers, params=params, timeout=300)
    response.raise_for_status()
    task_id = response.json().get('task_id')

    if not task_id:
        raise Exception("Failed to get a task ID from the video summarizer.")

    status_endpoint = f"{api_url}/status/{task_id}"
    while True:
        time.sleep(5)
        status_response = requests.get(status_endpoint, timeout=300)
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
    return result_text.strip()

def translate_text(text):
    """Calls the translator/rephraser API to translate text."""
    api_url = os.getenv("TRANSLATOR_REPHRASER_API_URL")
    payload = {"text": text, "task": "translate"}
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