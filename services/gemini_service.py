import os
import sys
import logging
import threading
import requests
import google.generativeai as genai
from google.generativeai.types import File
from typing import Optional, List, Dict, Any, Set
from dotenv import load_dotenv

# Load environment variables early
load_dotenv()

# Set up logger
logger = logging.getLogger(__name__)


# ============================================================
#  Gemini Key Manager — Round-Robin rotation
# ============================================================

class GeminiKeyManager:
    """
    Manages multiple Gemini API keys with Round-Robin rotation.
    - Loads keys from GEMINI_API_KEYS env var (comma-separated).
    - Falls back to single GEMINI_API_KEY or --gemini-api-key CLI arg.
    - Rotates to the next key on every call to get_next_key().
    - Marks keys as dead on quota errors, skips them in rotation.
    - Dynamic: works with any number of keys (1, 5, 12, 22...).
    """

    def __init__(self):
        self._keys: List[str] = []
        self._dead_keys: Set[int] = set()
        self._current_index: int = -1
        self._last_used_key: Optional[str] = None
        self._load_keys()

    def _load_keys(self):
        """Load keys from environment. Priority: GEMINI_API_KEYS > CLI arg > GEMINI_API_KEY."""
        # 1. Try comma-separated list (primary)
        keys_str = os.getenv("GEMINI_API_KEYS", "")
        if keys_str:
            self._keys = [k.strip() for k in keys_str.split(",") if k.strip()]
            if self._keys:
                logger.info(f"Loaded {len(self._keys)} Gemini API keys from GEMINI_API_KEYS")
                return

        # 2. Try CLI argument
        if "--gemini-api-key" in sys.argv:
            token_index = sys.argv.index("--gemini-api-key") + 1
            if token_index < len(sys.argv):
                self._keys = [sys.argv[token_index].strip()]
                logger.info("Loaded 1 Gemini API key from CLI argument")
                return

        # 3. Try single env var (fallback)
        single_key = os.getenv("GEMINI_API_KEY", "")
        if single_key:
            self._keys = [single_key.strip()]
            logger.info("Loaded 1 Gemini API key from GEMINI_API_KEY (fallback)")
            return

        logger.error("No Gemini API keys found! Set GEMINI_API_KEYS or GEMINI_API_KEY in .env")

    @property
    def total_keys(self) -> int:
        return len(self._keys)

    @property
    def alive_keys(self) -> int:
        return len(self._keys) - len(self._dead_keys)

    @property
    def all_exhausted(self) -> bool:
        return self.alive_keys <= 0

    @property
    def last_used_key(self) -> Optional[str]:
        return self._last_used_key

    def get_next_key(self) -> Optional[str]:
        """
        Returns the next alive API key using Round-Robin.
        Returns None if all keys are exhausted.
        """
        if not self._keys or self.all_exhausted:
            logger.warning("All Gemini API keys are exhausted!")
            return None

        for _ in range(self.total_keys):
            self._current_index = (self._current_index + 1) % self.total_keys
            if self._current_index not in self._dead_keys:
                key = self._keys[self._current_index]
                self._last_used_key = key
                logger.info(f"Using Gemini key #{self._current_index + 1}/{self.total_keys} (***{key[-6:]})")
                return key

        return None

    def mark_key_dead(self, key: Optional[str] = None):
        """
        Mark a key as exhausted. If no key provided, marks the last used key.
        """
        if key is None:
            key = self._last_used_key
        if key is None:
            return

        try:
            idx = self._keys.index(key)
            self._dead_keys.add(idx)
            logger.warning(f"Gemini key #{idx + 1} (***{key[-6:]}) marked DEAD. Alive: {self.alive_keys}/{self.total_keys}")
        except ValueError:
            pass

    def reset_all(self):
        """Reset all keys to alive (call at start of each new search session)."""
        self._dead_keys.clear()
        self._current_index = -1
        self._last_used_key = None
        logger.info(f"All {self.total_keys} Gemini keys reset to alive")

    def get_status(self) -> dict:
        return {
            "total": self.total_keys,
            "alive": self.alive_keys,
            "dead": len(self._dead_keys),
            "all_exhausted": self.all_exhausted,
        }


# Global singleton — created once on import
key_manager = GeminiKeyManager()


def get_gemini_api_key() -> str:
    """
    Get next Gemini API key using Round-Robin rotation.
    Backward-compatible wrapper around key_manager.
    """
    key = key_manager.get_next_key()
    if key is None:
        raise Exception("All Gemini API keys are exhausted")
    return key


def configure_gemini() -> genai.GenerativeModel:
    """
    Configure Gemini API with the next rotated API key and return a model.
    Each call picks the NEXT key in the Round-Robin cycle.
    """
    api_key = get_gemini_api_key()
    genai.configure(api_key=api_key)

    model = genai.GenerativeModel('gemini-3.1-flash-lite')

    logger.info("Gemini API configured successfully")
    return model


def upload_video_to_gemini(video_path: str, api_key: Optional[str] = None) -> Any:
    """
    Upload a video file to Gemini File API using REST for thread-safety.
    
    Args:
        video_path: Path to the video file to upload
        api_key: Specific API key to use
        
    Returns:
        A mock-like object with .uri and .name to maintain compatibility
    """
    if not api_key:
        api_key = get_gemini_api_key()

    import os
    import requests
    import json
    import time
    
    file_size = os.path.getsize(video_path)
    file_name_short = os.path.basename(video_path)

    # 1. Initial request to get upload URL (Resumable upload)
    # Using v1beta for File API
    setup_url = f"https://generativelanguage.googleapis.com/upload/v1beta/files?key={api_key}"
    headers = {
        "X-Goog-Upload-Protocol": "resumable",
        "X-Goog-Upload-Command": "start",
        "X-Goog-Upload-Header-Content-Length": str(file_size),
        "X-Goog-Upload-Header-Content-Type": "video/mp4",
        "Content-Type": "application/json"
    }
    
    # Optional metadata
    payload = {"file": {"display_name": file_name_short}}
    
    try:
        resp = requests.post(setup_url, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise Exception(f"Failed to start upload: {resp.text}")
            
        upload_url = resp.headers.get("X-Goog-Upload-URL")
        if not upload_url:
            raise Exception("No upload URL returned from Gemini")

        # 2. Upload actual data
        with open(video_path, 'rb') as f:
            headers = {
                "Content-Length": str(file_size),
                "X-Goog-Upload-Offset": "0",
                "X-Goog-Upload-Command": "upload, finalize"
            }
            resp = requests.post(upload_url, headers=headers, data=f, timeout=300)
            
        if resp.status_code != 200:
            raise Exception(f"Upload failed: {resp.text}")
            
        file_info = resp.json().get("file", {})
        file_name = file_info.get("name") # This is the full resource name like "files/abc"
        file_uri = file_info.get("uri")
        
        if not file_name:
            raise Exception("Upload succeeded but no file name returned")

        # 3. Wait for processing (REST version of processing loop)
        status_url = f"https://generativelanguage.googleapis.com/v1beta/{file_name}?key={api_key}"
        
        while True:
            status_resp = requests.get(status_url, timeout=20)
            if status_resp.status_code != 200:
                raise Exception(f"Status check failed: {status_resp.text}")
                
            status_info = status_resp.json()
            state = status_info.get("state")
            
            if state == "ACTIVE":
                logger.info(f"Video {file_name} is ACTIVE")
                # Create a compatibility object
                class GeminiFile:
                    def __init__(self, name, uri):
                        self.name = name
                        self.uri = uri
                return GeminiFile(file_name, file_uri)
            
            if state == "FAILED":
                raise Exception(f"Video processing failed: {status_info.get('error', 'Unknown Error')}")
                
            time.sleep(3)
            
    except Exception as e:
        logger.error(f"REST Video upload failed: {str(e)}")
        raise


def analyze_video_with_gemini(model: genai.GenerativeModel, video_file: File, prompt: str, api_key: Optional[str] = None) -> str:
    """
    Analyze a video using direct REST API call for thread-safety.
    
    Args:
        model: Not used in REST version, but kept for signature compatibility
        video_file: Uploaded video file object
        prompt: Analysis prompt
        api_key: The SPECIFIC key that uploaded the file
    """
    if not api_key:
        api_key = get_gemini_api_key()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}
    
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"file_data": {
                    "mime_type": "video/mp4", # Gemini usually treats all uploaded videos as video/mp4 format
                    "file_uri": video_file.uri
                }}
            ]
        }]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        res_json = response.json()
        
        if response.status_code != 200:
            error_details = res_json.get('error', {}).get('message', response.text)
            raise Exception(f"Gemini API Error {response.status_code}: {error_details}")

        # Extracting text from response structure
        candidates = res_json.get('candidates', [])
        if not candidates:
            # Check for safety ratings blocks
            if res_json.get('promptFeedback'):
                return "Analysis blocked by Google Safety Filters (Video)"
            # Check for Finish Reason
            return "Analysis blocked by safety filters (Video)"
            
        part = candidates[0].get('content', {}).get('parts', [{}])[0]
        text = part.get('text', '').strip()
        
        # If text is empty but candidate exists, check finishReason
        if not text:
            finish_reason = candidates[0].get('finishReason')
            if finish_reason == 'SAFETY':
                return "Analysis blocked by Gemini SAFETY filters (Video)"
            return "Error: Gemini returned empty text (Possibly blocked)"
            
        return text
        
    except Exception as e:
        logger.error(f"REST Video analysis failed: {str(e)}")
        raise


def cleanup_gemini_file(file_name: str):
    """
    Delete a single file from Gemini File API to free up storage.
    
    Args:
        file_name: Name of the file to delete
    """
    try:
        genai.delete_file(file_name)
        logger.info(f"Cleaned up Gemini file: {file_name}")
    except Exception as e:
        logger.warning(f"Failed to cleanup Gemini file {file_name}: {str(e)}")


def analyze_image_with_gemini(model: genai.GenerativeModel, image_bytes: bytes, prompt: str, mime_type: str = "image/jpeg", api_key: Optional[str] = None) -> str:
    """
    Analyze an image using direct REST API call for thread-safety.
    """
    if not api_key:
        api_key = get_gemini_api_key()

    import base64
    image_b64 = base64.b64encode(image_bytes).decode('utf-8')

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}
    
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {
                    "mime_type": mime_type,
                    "data": image_b64
                }}
            ]
        }]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=40)
        res_json = response.json()
        
        if response.status_code != 200:
            raise Exception(f"Gemini Image API Error {response.status_code}: {res_json.get('error', {}).get('message')}")

        candidates = res_json.get('candidates', [])
        if not candidates:
            # Check for safety ratings blocks
            if res_json.get('promptFeedback'):
                return "Analysis blocked by Google Safety Filters (Image)"
            return "Analysis blocked by safety filters (Image)"
            
        part = candidates[0].get('content', {}).get('parts', [{}])[0]
        text = part.get('text', '').strip()
        
        # If text is empty but candidate exists, check finishReason
        if not text:
            finish_reason = candidates[0].get('finishReason')
            if finish_reason == 'SAFETY':
                return "Analysis blocked by Gemini SAFETY filters (Image)"
            return "Error: Gemini Image returned empty text (Possibly blocked)"
            
        return text
        
    except Exception as e:
        logger.error(f"REST Image analysis failed: {str(e)}")
        raise


def analyze_videos_batch_with_gemini(model: genai.GenerativeModel, video_files: List[File], prompt_template: str, video_contexts: List[Dict[str, Any]]) -> List[str]:
    """
    Analyze multiple videos using Gemini in a single request for token efficiency.
    
    Args:
        model: Configured Gemini model instance
        video_files: List of uploaded video files from Gemini File API
        prompt_template: Base analysis prompt template
        video_contexts: List of context dicts with brand_name, ad_id, etc. for each video
        
    Returns:
        List[str]: Analysis results for each video in order
        
    Raises:
        Exception: If batch analysis fails
    """
    try:
        if not video_files or len(video_files) != len(video_contexts):
            raise Exception("Video files and contexts must have matching lengths")
        
        # Create batch prompt with multiple videos
        batch_prompt = f"""Analyze the following {len(video_files)} Facebook ad videos. For each video, provide analysis following this format:

{prompt_template}

Please analyze each video separately and clearly label each analysis as "VIDEO 1:", "VIDEO 2:", etc.

"""
        
        # Add context information for each video
        for i, context in enumerate(video_contexts, 1):
            brand_info = f" (Brand: {context.get('brand_name', 'Unknown')})" if context.get('brand_name') else ""
            ad_info = f" (Ad ID: {context.get('ad_id', 'Unknown')})" if context.get('ad_id') else ""
            batch_prompt += f"VIDEO {i}{brand_info}{ad_info}:\n"
        
        # Combine all video files with the prompt
        content_parts = [batch_prompt] + video_files
        
        # Generate batch analysis
        response = model.generate_content(content_parts)
        
        if not response.text:
            raise Exception("Gemini returned empty response for batch analysis")
        
        # Split response by video markers
        analysis_text = response.text
        video_analyses = []
        
        # Parse individual video analyses
        for i in range(1, len(video_files) + 1):
            video_marker = f"VIDEO {i}:"
            next_marker = f"VIDEO {i + 1}:" if i < len(video_files) else None
            
            start_idx = analysis_text.find(video_marker)
            if start_idx == -1:
                logger.warning(f"Could not find analysis for VIDEO {i}")
                video_analyses.append(f"Analysis not found in batch response for video {i}")
                continue
                
            start_idx += len(video_marker)
            
            if next_marker:
                end_idx = analysis_text.find(next_marker)
                individual_analysis = analysis_text[start_idx:end_idx].strip() if end_idx != -1 else analysis_text[start_idx:].strip()
            else:
                individual_analysis = analysis_text[start_idx:].strip()
            
            video_analyses.append(individual_analysis)
        
        logger.info(f"Batch video analysis completed successfully for {len(video_files)} videos")
        return video_analyses
        
    except Exception as e:
        logger.error(f"Batch video analysis failed: {str(e)}")
        raise


def upload_videos_batch_to_gemini(video_paths: List[str]) -> List[File]:
    """
    Upload multiple video files to Gemini File API for batch analysis.
    
    Args:
        video_paths: List of paths to video files to upload
        
    Returns:
        List[genai.File]: List of uploaded file objects for use in analysis
        
    Raises:
        Exception: If any upload fails
    """
    uploaded_files = []
    failed_uploads = []
    
    try:
        for i, video_path in enumerate(video_paths):
            try:
                # Upload video file
                video_file = genai.upload_file(path=video_path)
                
                # Wait for processing to complete
                import time
                while video_file.state.name == "PROCESSING":
                    time.sleep(2)
                    video_file = genai.get_file(video_file.name)
                
                if video_file.state.name == "FAILED":
                    failed_uploads.append(f"Video {i+1}: {video_file.state}")
                    continue
                    
                uploaded_files.append(video_file)
                logger.info(f"Video {i+1} uploaded successfully: {video_file.name}")
                
            except Exception as e:
                failed_uploads.append(f"Video {i+1}: {str(e)}")
                logger.error(f"Failed to upload video {i+1} at {video_path}: {str(e)}")
        
        if failed_uploads:
            error_msg = f"Some video uploads failed: {'; '.join(failed_uploads)}"
            if not uploaded_files:  # All uploads failed
                raise Exception(error_msg)
            else:  # Partial failure
                logger.warning(error_msg)
        
        return uploaded_files
        
    except Exception as e:
        # Cleanup any successfully uploaded files on total failure
        for uploaded_file in uploaded_files:
            try:
                cleanup_gemini_file(uploaded_file.name)
            except:
                pass
        raise


def cleanup_gemini_files_batch(file_names: List[str]):
    """
    Delete multiple files from Gemini File API to free up storage.
    
    Args:
        file_names: List of file names to delete
    """
    for file_name in file_names:
        try:
            genai.delete_file(file_name)
            logger.info(f"Cleaned up Gemini file: {file_name}")
        except Exception as e:
            logger.warning(f"Failed to cleanup Gemini file {file_name}: {str(e)}")


def cleanup_gemini_file(file_name: str):
    """
    Delete a file from Gemini File API to free up storage.
    
    Args:
        file_name: Name of the file to delete
    """
    try:
        genai.delete_file(file_name)
        logger.info(f"Cleaned up Gemini file: {file_name}")
    except Exception as e:
        logger.warning(f"Failed to cleanup Gemini file {file_name}: {str(e)}")


def analyze_images_batch_with_gemini(image_data_list: List[Dict[str, Any]], primary_text: str = "", api_key: Optional[str] = None) -> Optional[str]:
    """
    Batch image analysis using REST API (Thread-safe).
    """
    if not image_data_list:
        return None

    if not api_key:
        api_key = get_gemini_api_key()

    import base64
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}

    prompt = f"""
ПРОАНАЛИЗИРУЙ ЭТИ {len(image_data_list)} КАРТИНКИ ИЗ ОДНОГО ОБЪЯВЛЕНИЯ В ФЕЙСБУКЕ.
Они могут быть частью одной Карусели или Динамического креатива.

ТВОЯ ЗАДАЧА:
1. Описать каждую картинку отдельно.
2. Для каждой картинки начни блок со строки "CARD X:" (где X - номер картинки).

ПРАВИЛА: 
1. ОФФЕР это БАДы, препараты, лекарства: мази, гели, капли, капсулы, таблетки, спреи, чаи, порошки для здоровья. ВСЁ ОСТАЛЬНОЕ - ЭТО "white". ВАЖНО: Если ОФФЕР="white", полностью пропусти пункт 2 для этой карточки.
1.1 Запомни что "white" - это когда креатив продает онлайн-курсы, приложения, услуги врача и клиник, устройства, одежду, животных, растения, еду.

СТРУКТУРА ДЛЯ КАЖДОЙ КАРТОЧКИ:
CARD X:
1. ОФФЕР: [Конкретное название бренда, препарата и форма (например, 'Prostovit капсулы'). Если бренд не читается, но это явно медицина — 'неизвестный [препарат/форма]'. Если это белая заглушка/гном/овощ/животное/одежда и не является БАДом, лекарством, препаратом — ОБЯЗАТЕЛЬНО укажи 'white'].
2. СОДЕРЖАНИЕ: [Максимально детально: ВЕСЬ ТЕКСТ на картинке, все персонажи, их позы, действия, одежда, все визуальные элементы и предметы].
"""
    
    parts = [{"text": prompt}]
    if primary_text:
        parts.append({"text": f"AD TEXT FOR CONTEXT: {primary_text}"})

    for i, img_data in enumerate(image_data_list):
        parts.append({"text": f"IMAGE {i+1}:"})
        parts.append({
            "inline_data": {
                "mime_type": img_data['mime_type'],
                "data": base64.b64encode(img_data['bytes']).decode('utf-8')
            }
        })

    payload = {"contents": [{"parts": parts}]}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        res_json = response.json()
        
        if response.status_code != 200:
            err_msg = res_json.get('error', {}).get('message', 'Unknown Error')
            if "429" in str(response.status_code) or "quota" in err_msg.lower():
                key_manager.mark_key_dead(api_key)
                return analyze_images_batch_with_gemini(image_data_list, primary_text) # Retry with next key
            return f"Error: {err_msg}"

        candidates = res_json.get('candidates', [])
        if not candidates:
            return "Analysis blocked by Google Safety Filters (Batch - Empty Candidates)"
            
        text = candidates[0].get('content', {}).get('parts', [{}])[0].get('text', '').strip()
        if not text:
             # Fallback: check if the first candidate has a block reason
             finish_reason = candidates[0].get('finishReason')
             if finish_reason:
                  return f"Analysis blocked by Gemini Safety filters (Batch - {finish_reason})"
             return "Error: Empty response from Gemini (Batch)"
             
        return text
    except Exception as e:
        return f"Error: {str(e)}"
