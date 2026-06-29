
from services.scrapecreators_service import get_platform_id, get_ads, get_scrapecreators_api_key, get_platform_ids_batch, get_ads_batch, CreditExhaustedException, RateLimitException, search_ads_by_keyword, parse_fb_ads, ADS_API_URL, check_credit_status
from services.media_cache_service import media_cache, image_cache
from services.gemini_service import configure_gemini, upload_video_to_gemini, analyze_video_with_gemini, cleanup_gemini_file, analyze_videos_batch_with_gemini, upload_videos_batch_to_gemini, cleanup_gemini_files_batch, get_gemini_api_key, analyze_image_with_gemini, key_manager
from typing import Dict, Any, List, Optional, Union
from collections import defaultdict, Counter
import requests
import base64
import os
import json
import logging
import threading
from dotenv import load_dotenv
import sys
from datetime import datetime
import re

# Configure logging to stderr
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp_library")

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(__file__), '.env')
if not os.path.exists(env_path):
    # Try parent directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(env_path)

# Gemini quota tracking is now handled by key_manager (Round-Robin) in gemini_service.py

# Check Gemini availability
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except Exception:
    GEMINI_AVAILABLE = False

# Excluded domains
EXCLUDED_DOMAINS = [
    # General Marketplaces (Global/EU/LATAM)
    'amazon', 'amzn', 'ebay', 'aliexpress', 'alibaba', 'temu', 'shein', 'shopee', 'dhgate',
    'mercadolibre', 'mercadolivre', 'mercadopago', # LATAM giants
    'falabella', 'linio', 'liverpool.com.mx', 'coppel', 'walmart', 'carrefour', 'elcorteingles', # Retailers
    'wish.com', 'etsy', 'rakuten', 'zalando', 'asos', 'allegro', 'cdiscount', 'fnac', 'bol.com', # EU/Global
    
    # App Stores & Digital Content
    'play.google.com', 'apps.apple.com', 'itunes.apple.com', 'app.apple.com',
    'store.steampowered', 'epicgames', 'microsoft.com/store',
    'wattpad', 'webtoon', 'goodreads', 'audible', '*.reader', '*.book',
    
    # Educational & Courses
    'hotmart', 'udemy', 'coursera', 'teachable', 'skillshare', 'masterclass', 'domestika', 'crehana',
    
    # Payment & Services
    'pay.', 'paypal', 'stripe', 'shopify.com', # myshopify is tricky, sometimes used for landings, but usually 'checkout' path filters it
    
    # Social & Messaging & Video (Internal/External)
    'facebook', 'fb.me', 'fb.com', 'instagram', 'whatsapp', 'wa.me', 'wa.link', 'messenger',
    'api.whatsapp', 'chat.whatsapp',
    'twitter', 'x.com', 'tiktok', 'snapchat', 'pinterest', 'linkedin', 'reddit', 'tumblr',
    'youtube', 'youtu.be', 'vimeo', 'dailymotion', 'twitch',
    't.me', 'telegram', 'discord',
    
    # Google Services
    'google', 'g.co', 'goo.gl', 'maps.app.goo.gl', 'forms.gle', 'drive.google', 'docs.google',
    
    # Medical/Wellness (Official/Telehealth)
    'betterhelp', 'talkspace', 'doctoralia', 'mayoclinic', 'webmd', 'healthline',
    'network.mynursingcommunity.com',
    
    # Sports/Branded Fitness
    'nike', 'adidas', 'puma', 'underarmour', 'reebok', 'gymshark', 'decathlon',
    'myfitnesspal', 'strava'
]

# Excluded URL paths
EXCLUDED_URL_PATHS = [
    '/curso/', '/programa/', '/curso-online/', '/training/', '/academy/',
    '/marketplace/', '/cart/', '/checkout/',
    '/psycholog', '/therapy/', '/counseling/', '/hypnosis/',
    '/fitness/', '/gym/', '/workout/'
]


def is_excluded_domain(domain: str) -> bool:
    """Checks if a domain is excluded."""
    if not domain:
        return False
    domain_lower = domain.lower()
    for excluded in EXCLUDED_DOMAINS:
        if excluded.replace('*', '') in domain_lower:
            return True
    return False


def is_excluded_url(url: str) -> bool:
    """Checks if a URL contains excluded paths."""
    if not url:
        return False
    url_lower = url.lower()
    for path in EXCLUDED_URL_PATHS:
        if path in url_lower:
            return True
    return False


def _is_excluded_by_text(text: str) -> bool:
    """
    Fast heuristic text-only check (no Gemini). Catches obvious junk.
    Returns True if ad should be excluded based on keywords.
    """
    if not text:
        return False
    text_lower = text.lower()
    # Hard exclusion keywords
    exclusion_keywords = [
        'udemy', 'coursera', 'hotmart', 'teachable', 'domestika',
        'hypnosis', 'hypnother', 'гипноз', 'гипнотерап',
        'онлайн-курс', 'online course', 'webinar', 'вебинар',
        'мастер-класс', 'masterclass',
    ]
    return any(kw in text_lower for kw in exclusion_keywords)


def filter_ad(ad: Dict[str, Any]) -> bool:
    """Fast structural ad filtering — no Gemini calls. Check domain/URL/text heuristics."""
    ad_id = ad.get('ad_id', 'unknown')
    if not ad.get('has_external_links'):
        logger.info(f"Skipping ad {ad_id}: No external links found")
        return False
    
    external_urls = ad.get('external_urls', [])
    if not external_urls:
        logger.info(f"Skipping ad {ad_id}: external_urls array is empty")
        return False
    
    has_valid_link = False
    
    for url_obj in external_urls:
        url = url_obj.get('full_url', '')
        domain = url_obj.get('domain', '')
        
        if not url:
            continue
            
        if not is_excluded_domain(domain) and not is_excluded_url(url):
            has_valid_link = True
            break
            
    if not has_valid_link:
        logger.info(f"Skipping ad {ad_id}: All URLs/domains are in exclusion list")
        return False
    
    body_text = ad.get('body', '') or ''
    title_text = ad.get('title', '') or ''
    combined = f"{title_text}\n{body_text}"
    
    if len(combined) > 4000:
        logger.info(f"Skipping ad {ad_id}: Body text too long ({len(combined)} chars)")
        return False
    
    if _is_excluded_by_text(combined):
        logger.info(f"Skipping ad {ad_id}: Text content contains exclusion keywords")
        return False
    
    return True


def parse_batch_response(batch_text: str, num_cards: int) -> List[str]:
    """Parses Gemini batch response into individual analyses. Robust for various formats."""
    import re
    if not batch_text:
        return [f"Error: Empty response from Gemini"] * num_cards
    
    # Propagate explicit errors to all cards
    if batch_text.startswith("Error:"):
        return [batch_text] * num_cards
        
    parts = []
    
    # CASE 1: Only one card expected - flexibility is key.
    if num_cards == 1:
        # Check for card title in various formats: CARD 1, **CARD 1**, ### CARD 1
        pattern = r"(?:^|\n)\s*(?:\*\*|###)?\s*CARD\s*\**1\**\s*:?\s*(.*)"
        match = re.search(pattern, batch_text, re.DOTALL | re.IGNORECASE)
        if match:
            text = match.group(1).strip()
            text = re.sub(r'^[:\s\n*]+', '', text)
            return [text]
        return [batch_text.strip()]

    # CASE 2: Multiple cards.
    for i in range(1, num_cards + 1):
        # Match from "CARD i" (with possible markdown) to "CARD i+1" or end of text.
        # Uses a non-greedy catch-all (.*?) and a lookahead for the next CARD marker.
        pattern = rf"(?:\*\*|###)?\s*CARD\s*\**{i}\**\s*:?\s*(.*?)(?=(?:\*\*|###)?\s*CARD\s*\**{i+1}\**\s*:?|$)"
        match = re.search(pattern, batch_text, re.DOTALL | re.IGNORECASE)
        if match:
            text = match.group(1).strip()
            # Clean up leading colon or bullet points often inserted by Gemini
            text = re.sub(r'^[:\s\n*]+', '', text)
            parts.append(text)
        else:
            # Fallback split if regex fails: search for any mention of the card index
            parts.append(f"Analysis for card {i} not found in batch response.")
            
    return parts

def analyze_ad_media_batch(ads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Analyzes a group of ads (cards for same ID) using Gemini batching."""
    if not ads:
        return ads
        
    import requests
    from services.gemini_service import analyze_images_batch_with_gemini
    
    ad_text = ads[0].get('body', '')
    
    # Use ThreadPoolExecutor for concurrent image downloads
    from concurrent.futures import ThreadPoolExecutor
    
    def download_image(ad):
        murl = ad.get('media_url', '')
        if ad.get('media_type') == 'IMAGE' and murl:
            try:
                # Check cache first
                from services.media_cache_service import media_cache
                from pathlib import Path
                cached = media_cache.get_cached_media(murl.strip(), media_type='image')
                if cached and Path(cached['file_path']).exists():
                    return {
                        'bytes': Path(cached['file_path']).read_bytes(),
                        'mime_type': cached.get('content_type', 'image/jpeg')
                    }
                
                resp = requests.get(murl, timeout=10)
                if resp.status_code == 200:
                    c_type = resp.headers.get('content-type', 'image/jpeg')
                    # Save to cache
                    media_cache.cache_media(
                        url=murl.strip(),
                        media_data=resp.content,
                        content_type=c_type,
                        media_type='image',
                        brand_name=ad.get('page_name'),
                        ad_id=ad.get('ad_id')
                    )
                    return {
                        'bytes': resp.content,
                        'mime_type': c_type
                    }
            except Exception as e:
                print(f"Error downloading/caching image {murl}: {e}", file=sys.stderr)
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        images_to_batch = list(executor.map(download_image, ads))

    actual_images = [img for img in images_to_batch if img is not None]
    print(f"DEBUG: Successfully downloaded {len(actual_images)}/{len(ads)} images for Ad ID {ads[0]['ad_id']}", file=sys.stderr)
    
    # Get a fixed key for this ad group to avoid 403 errors in threads and respect RPM
    assigned_key = get_gemini_api_key()

    parsed_analyses = []
    if actual_images:
        print(f"DEBUG: [Thread {threading.get_ident()}] Batching {len(actual_images)} images for Ad ID {ads[0]['ad_id']} using key ...{assigned_key[-6:]}", file=sys.stderr)
        
        batch_text = analyze_images_batch_with_gemini(actual_images, ad_text, api_key=assigned_key)
        print(f"--- GEMINI RAW START (ID: {ads[0].get('ad_id')}) ---\n{batch_text}\n--- GEMINI RAW END ---", file=sys.stderr)
        
        parsed_analyses = parse_batch_response(batch_text, len(actual_images))
        
    img_counter = 0
    for i, ad in enumerate(ads):
        if images_to_batch[i] is not None:
            analysis_text = parsed_analyses[img_counter] if img_counter < len(parsed_analyses) else ""
            ad['media_analysis'] = {
                'image_analysis': {'raw_analysis': analysis_text}
            }
            img_counter += 1
        else:
            # If it's a video, analyze individually with SAME key
            if ad.get('media_type') == 'VIDEO':
                murl = ad.get('media_url', '')
                if murl:
                    print(f"DEBUG: [Thread {threading.get_ident()}] Analyzing VIDEO for Ad ID {ad['ad_id']} with key ...{assigned_key[-6:]}", file=sys.stderr)
                    # Use existing library for upload (still okay), but REST for analysis
                    video_res = analyze_ad_video(
                        media_url=murl, 
                        brand_name=ad.get('page_name'), 
                        ad_id=ad['ad_id'], 
                        ad_text=ad.get('body', ''),
                        api_key=assigned_key # NEW: Pass key to avoid global conflict
                    )
                    if video_res.get('success'):
                        ad['media_analysis'] = video_res.get('analysis', {})
                    else:
                        error_msg = video_res.get('error', 'Unknown video analysis error')
                        print(f"DEBUG: Video analysis FAILED for Ad ID {ad['ad_id']}: {error_msg}", file=sys.stderr)
                        ad['media_analysis'] = {
                            'analysis_error': error_msg
                        }
            elif ad.get('media_type') == 'IMAGE':
                ad['media_analysis'] = {
                    'analysis_error': 'IMAGE analysis SKIPPED (download failed)'
                }
            else:
                ad['media_analysis'] = {
                    'analysis_error': f"Media type {ad.get('media_type')} not supported for analysis."
                }
    
    return ads

def detect_heuristics(ads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Detects URL and format anomalies in a group of ads and adds *** markers."""
    if len(ads) <= 1:
        return ads
        
    # 1. URL/Domain Anomaly
    domains = []
    for ad in ads:
        ext_urls = ad.get('external_urls', [])
        if ext_urls:
            url_obj = ext_urls[0]
            domain = url_obj.get('domain') if isinstance(url_obj, dict) else ""
            domains.append(domain)
        else:
            domains.append(None)
            
    from collections import Counter
    domain_counts = Counter([d for d in domains if d])
    if domain_counts:
        main_domain = domain_counts.most_common(1)[0][0]
        # Heuristic check: if there's a domain variance, keep it but don't add stars.
        # Logic removed as per user request to remove stars.
        pass

    # 2. Format Anomaly (Video among images)
    # Logic removed as per user request to remove stars.

    return ads


def analyze_media_func(ad: dict, analyze_media: bool) -> Dict[str, Any]:
    """
    Analyzes ad media with a single Gemini call that combines:
    - Media analysis (visual/scene description)
    - Content classification (INCLUDE/EXCLUDE decision)
    All in one request. Returns analysis results.
    """

    
    media_type = ad.get('media_type', '')
    media_url = ad.get('media_url', '')
    ad_id = ad.get('ad_id', '')
    ad_text = (ad.get('title', '') or '') + '\n' + (ad.get('body', '') or '')
    ad_text = ad_text.strip()
    
    analysis_result = {
        'image_analysis': None,
        'video_analysis': None,
        'analysis_error': None
    }
    
    if key_manager.all_exhausted:
        analysis_result['analysis_error'] = f"Analysis skipped: All {key_manager.total_keys} Gemini keys exhausted."
        return analysis_result

    if not media_url:
        analysis_result['analysis_error'] = "No media URL provided."
        return analysis_result
    
    try:
        if media_type.upper() in ('VIDEO',):
            result = analyze_ad_video(media_url=media_url, brand_name=None, ad_id=ad_id, ad_text=ad_text)
            if result.get('success'):
                analysis_result['video_analysis'] = result.get('analysis', {})
            else:
                error = result.get('error', 'Unknown error')
                analysis_result['analysis_error'] = error
                error_str = str(error).lower()
                if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
                    key_manager.mark_key_dead()
        
        elif media_type.upper() in ('IMAGE', 'DCO', 'CAROUSEL', 'DPA', 'MULTI_IMAGES'):
            result = analyze_ad_image(media_urls=media_url, brand_name=None, ad_id=ad_id, ad_text=ad_text)
            if result.get('success'):
                if result.get('analysis'):
                    analysis_result['image_analysis'] = result.get('analysis', {})
                elif result.get('image_data'):
                    analysis_result['image_analysis'] = {
                        'has_image_data': True,
                        'analysis_instructions': result.get('analysis_instructions', '')
                    }
            else:
                error = result.get('error', 'Unknown error')
                analysis_result['analysis_error'] = error
                error_str = str(error).lower()
                if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
                    key_manager.mark_key_dead()
        else:
            analysis_result['analysis_error'] = f"Unsupported media type: {media_type}"
    
    except Exception as e:
        error_str = str(e).lower()
        analysis_result['analysis_error'] = str(e)
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503']):
            key_manager.mark_key_dead()
    
    return analysis_result


def deduplicate_ads(ads_by_url: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Deduplicates ads. Since we now want all cards, we actually just flatten the list
    but can still use this to ensure no exact duplicates (ad_id + media_url).
    """
    seen = set()
    unique_ads = []
    
    for _, ads_list in ads_by_url.items():
        for ad in ads_list:
            key = (ad.get('ad_id'), ad.get('media_url'))
            if key not in seen:
                unique_ads.append(ad)
                seen.add(key)
    
    return unique_ads


def convert_ad_to_file_format(ad: dict) -> dict:
    """Converts ad to file format."""
    ad_data = {
        'ad_id': ad.get('ad_id'),
        'ad_text': ad.get('body'),
        'external_urls': list(dict.fromkeys([u.get('full_url') if isinstance(u, dict) else u for u in ad.get('external_urls', [])])),
        'fanpage_url': f"https://www.facebook.com/{ad.get('page_id', '')}" if ad.get('page_id') else None,
        'ad_url': f"https://www.facebook.com/ads/library/?id={ad.get('ad_id')}" if ad.get('ad_id') else None,
        'page_name': ad.get('page_name'),
        'start_date': ad.get('start_date'),
        'end_date': ad.get('end_date'),
        'media_type': ad.get('media_type'),
        'display_format': ad.get('display_format'),
        'media_url': ad.get('media_url'),
        'search_query': ad.get('search_query'),
        'url_occurrences': ad.get('url_occurrences', 1),
        'media_analysis': ad.get('media_analysis', {})
    }
    title = ad.get('title', '')
    if title and title.strip():
        ad_data['title'] = title
    
    return ad_data


def load_existing_ads(filepath: str) -> tuple:
    """Loads existing ads from file and returns set of (ad_id, media_url) keys."""
    if not os.path.exists(filepath):
        return set(), []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            existing_ads = json.load(f)
    except Exception:
        return set(), []
    
    existing_keys = set()
    for ad in existing_ads:
        aid = ad.get('ad_id')
        murl = ad.get('media_url')
        if aid and murl:
            existing_keys.add((str(aid), str(murl)))
    
    return existing_keys, existing_ads


def save_results(ads: list, filename: str):
    """Saves results to JSON file using ABSOLUTE PATH."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Path: manual_server -> facebook-ads-library-mcp -> myspy

    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Self-contained: Save results inside the server directory
    results_dir = os.path.join(current_dir, 'results')
    
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    filepath = os.path.join(results_dir, filename)
    print(f"DEBUG: Saving to {filepath}", file=sys.stderr) # Force debug output
    
    output_data = []
    for ad in ads:
        if isinstance(ad, dict) and 'ad_id' in ad:
            output_data.append(ad)
        else:
            output_data.append(convert_ad_to_file_format(ad))
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    return filepath


def filter_new_ads(ads: list, existing_keys: set, max_ads: int = None) -> list:
    """Filters ads keeping only new ones based on (ad_id, media_url)."""
    new_ads = []
    for ad in ads:
        aid = ad.get('ad_id')
        murl = ad.get('media_url')
        if not aid or not murl:
            continue
            
        key = (str(aid), str(murl))
        if key not in existing_keys:
            new_ads.append(ad)
            existing_keys.add(key)
            if max_ads and len(new_ads) >= max_ads:
                break
    
    return new_ads

# --- EXPORTED TOOLS ---

def get_meta_platform_id(brand_names: Union[str, List[str]]) -> Dict[str, Any]:
    if isinstance(brand_names, str):
        if not brand_names or not brand_names.strip():
            return {"success": False, "message": "Brand name invalid", "results": {}, "total_results": 0}
        brand_list = [brand_names.strip()]
        is_single = True
    elif isinstance(brand_names, list):
        if not brand_names:
            return {"success": False, "message": "Brand names list empty", "results": {}, "total_results": 0}
        brand_list = [str(name).strip() for name in brand_names if name]
        is_single = False
    else:
        return {"success": False, "message": "Invalid input type", "results": {}, "total_results": 0}

    batch_info = None
    try:
        get_scrapecreators_api_key()
        if is_single:
            platform_ids = get_platform_id(brand_list[0])
            results = platform_ids
            total_found = len(platform_ids)
            batch_info = None
        else:
            batch_results = get_platform_ids_batch(brand_list)
            results = batch_results
            total_found = sum(len(ids) for ids in batch_results.values())
            successful = sum(1 for ids in batch_results.values() if ids)
            batch_info = {
                "total_requested": len(brand_list),
                "successful": successful,
                "failed": len(brand_list) - successful,
                "api_calls_used": len(brand_list)
            }
        
        return {
            "success": True,
            "message": f"Found {total_found} platform IDs.",
            "results": results,
            "batch_info": batch_info,
            "total_results": total_found,
            "error": None
        }
    except Exception as e:
        return {"success": False, "message": str(e), "results": {}, "total_results": 0, "error": str(e)}


def search_facebook_ads(
    query: str,
    limit: Optional[int] = 100,
    country: Optional[str] = None,
    active_status: str = "ACTIVE",
    media_type: str = "ALL",
    analyze_media: bool = True,
    target_file: Optional[str] = None,
    append_mode: bool = False,
    max_ads: Optional[int] = None,
    apply_filtering: bool = True,
    start_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Unified function to search for Facebook ads with media analysis and filtering.
    
    Args:
        query: Search keywords.
        limit: Max ads to fetch (forced min 100 for credit efficiency).
        country: Country code.
        active_status: "ACTIVE", "ALL".
        media_type: "ALL", "IMAGE", "VIDEO".
        analyze_media: Enable Gemini analysis.
        target_file: Filename to save/append results.
        append_mode: True to append, False to overwrite.
        max_ads: Limit saved ads count.
        apply_filtering: Enable domain/content logic filtering.
        start_date: Filter: ads that started after (YYYY-MM-DD).
    """
    key_manager.reset_all()

    if not query or not query.strip():
        return {"success": False, "message": "Missing query", "results": [], "count": 0}

    try:
        logging.info(f"Starting search_facebook_ads for query: {query}")
        get_scrapecreators_api_key()
        
        # User always wants at least 100 ads
        req_limit = limit if limit and limit >= 100 else 100
        if max_ads and max_ads > req_limit:
            req_limit = max_ads
        
        logging.info(f"Fetching {req_limit} ads from API")
        
        ads = search_ads_by_keyword(
            query=query,
            limit=req_limit,
            country=country,
            active_status=active_status,
            media_type=media_type,
            trim=False,
            start_date=start_date
        )
        
        if not ads:
             return {"success": True, "message": f"No ads found for query: {query}", "results": [], "count": 0}

        # Add search_query to each ad (fixes null issue)
        for ad in ads:
            ad['search_query'] = query

        # 1. Group ads by ad_id to process all cards (variants) together
        groups = defaultdict(list)
        for ad in ads:
            groups[ad['ad_id']].append(ad)
        
        # 2. Process each group in parallel (Stable Multi-threading + REST API)
        from concurrent.futures import ThreadPoolExecutor
        final_processed_ads = []
        
        def process_single_group(group_data):
            ad_id, group = group_data
            if apply_filtering:
                # 1. Structural filtering first (fast)
                group = [ad for ad in group if filter_ad(ad)]
                if not group:
                    return []
                # 2. Heuristics
                group = detect_heuristics(group)
                if not group:
                    return []
            
            # 3. Custom Health/Nutra Heuristic: exclude campaigns with >12 variants
            # User confirmed that target grey-hat health advertisers rarely use large >12 image DCOs.
            # Large DCOs are typically "white" advertisers (clinics, e-commerce).
            if len(group) > 12:
                print(f"DEBUG: Auto-skipping ad_id {ad_id}. Contains {len(group)} variants (>12), which indicates a 'white' advertiser.", file=sys.stderr)
                return []
            
            if analyze_media and group:
                group = analyze_ad_media_batch(group)
            return group

        # ThreadPool works fine now because we use direct REST API with fixed keys
        with ThreadPoolExecutor(max_workers=10) as executor:
            group_list = list(groups.items())
            total_groups = len(group_list)
            print(f"DEBUG: Processing {total_groups} groups in threads (Isolation via REST API)...", file=sys.stderr)
            
            processed_groups = list(executor.map(process_single_group, group_list))
            
            for idx, group in enumerate(processed_groups, 1):
                if idx % 10 == 0 or idx == total_groups:
                    print(f"PROGRESS: Processed {idx}/{total_groups} groups...", file=sys.stderr)
                for ad in group:
                    final_processed_ads.append(ad)
        
        # Format results without deduplication so that ALL variants (cards) are kept
        formatted_ads = [convert_ad_to_file_format(ad) for ad in final_processed_ads]
        
        # Saving results (Automatic if no target_file provided)
        # Priority: 
        # 1. target_file (if provided)
        # 2. auto-generated filename (if not provided)
        
        saved_filepath = None
        
        if formatted_ads:
             try:
                 if target_file:
                     filename_only = os.path.basename(target_file)
                 else:
                     # Auto-generate filename: Unified COUNTRY-based file
                     # ads_found_{COUNTRY}.json
                     country_code = country if country else "ALL"
                     filename_only = f"ads_found_{country_code}.json"
                     # Always append to create a consolidated database per country
                     append_mode = True 
                 
                 final_ads_to_save = formatted_ads
                 
                 # Logic for appending (whether explicit or auto)
                 if append_mode:
                     check_path = os.path.join("results", filename_only) 
                     # Note: save_results handles absolute path, but here we need to check existence.
                     # Since we moved 'results' to absolute path in save_results, we should do same here.
                     
                     current_dir = os.path.dirname(os.path.abspath(__file__))
                     results_dir_abs = os.path.join(current_dir, 'results')
                     check_path_abs = os.path.join(results_dir_abs, filename_only)
                     
                     if os.path.exists(check_path_abs):
                        try:
                            # IMPORTANT: load_existing_ads now returns (existing_keys, existing_ads)
                            # where existing_keys is a set of (ad_id, media_url)
                            existing_keys, existing_ads = load_existing_ads(check_path_abs)
                            
                            # Filter using NEW key logic (ad_id + media_url)
                            new_unique_ads = filter_new_ads(formatted_ads, existing_keys, max_ads)
                            
                            final_ads_to_save = existing_ads + new_unique_ads
                            saved_count = len(new_unique_ads)
                        except Exception as e:
                            logger.error(f"Append failed: {e}")
                 
                 saved_filepath = save_results(final_ads_to_save, filename_only)
                 logging.info(f"Saved results to: {saved_filepath}")
             except Exception as save_err:
                 logging.error(f"Saving failed: {save_err}")
                 saved_filepath = f"ERROR_SAVING: {save_err}"
             
             saved_count = len(final_ads_to_save) # Total in file if overwrite, or added count if append logic was clearer?
             # Let's keep saved_count as total items in file for clarity to user
             pass

        return {
            "success": True,
            "message": f"Found {len(formatted_ads)} ads (FIXED). Saved to {saved_filepath}.",
            "results": formatted_ads,
            "count": len(formatted_ads), # Return count of found ads in this run
            "total_found": len(ads),
            "saved_file": saved_filepath
        }

    except Exception as e:
        return {"success": False, "message": str(e), "results": [], "count": 0, "error": str(e)}




def get_meta_ads_external_only(platform_ids: Union[str, List[str]], limit: Optional[int] = 50, country: Optional[str] = None, min_results: Optional[int] = None) -> Dict[str, Any]:
    """Retrieve ads for brand(s) that lead to external websites (not Meta/Google properties)."""
    # Normalize: MCP may pass numeric IDs
    if isinstance(platform_ids, (int, float)):
        platform_ids = str(int(platform_ids))

    if isinstance(platform_ids, str):
        platform_list = [platform_ids.strip()]
        is_single = True
    elif isinstance(platform_ids, list):
        platform_list = [str(int(pid) if isinstance(pid, (int, float)) else pid).strip() for pid in platform_ids if pid]
        is_single = False
    else:
        return {"success": False, "message": "Invalid platform_ids type"}

    try:
         # Use get_ads logic internally... simplified here for brevity as exact copy is complex
         # Ideally we delegate to proper calls
         # For manual server, let's just implement the core call
         pass
         # Implementing essentially same logic as above tool
         # ... (Implementation omitted for brevity, but would match mcp_server.py logic)
         # For now, I will return a placeholder to ensure the function exists, 
         # but realistically user wants the search_medical functionality primarily.
         # Actually, I should probably copy the logic to be safe.
         
         fetch_limit = limit or 50
         if min_results and min_results > fetch_limit:
             fetch_limit = min(min_results * 2, 500)
             
         if is_single:
             all_ads = get_ads(platform_list[0], fetch_limit, country, trim=False)
             external_ads = [ad for ad in all_ads if ad.get('has_external_links', False)]
             return {"success": True, "results": external_ads[:limit], "count": len(external_ads[:limit])}
         else:
             batch = get_ads_batch(platform_list, fetch_limit, country, trim=False)
             external_results = {}
             for pid, ads in batch.items():
                 external_results[pid] = [ad for ad in ads if ad.get('has_external_links', False)][:limit]
             return {"success": True, "results": external_results}
             
    except Exception as e:
        return {"success": False, "message": str(e)}

def _fetch_all_ads_from_page(page_id: str, limit: int = 50, country: Optional[str] = None) -> List[Dict[str, Any]]:
    """Internal helper to fetch ads with filter_inactive=False, matching search_ads_final behavior."""
    api_key = get_scrapecreators_api_key()
    headers = {
        "x-api-key": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    params = {
        "pageId": str(page_id),
        "limit": min(limit, 1000)
    }
    if country:
        params["country"] = country.upper()
    
    ads = []
    cursor = None
    total_requests = 0
    
    while len(ads) < limit and total_requests < 10:
        if cursor:
            params['cursor'] = cursor
            
        try:
            response = requests.get(ADS_API_URL, headers=headers, params=params, timeout=30)
            total_requests += 1
            check_credit_status(response)
            if response.status_code != 200:
                break
                
            resJson = response.json()
            # USE filter_inactive=False!
            res_ads = parse_fb_ads(resJson, trim=False, filter_inactive=False)
            if not res_ads:
                break
                
            ads.extend(res_ads)
            cursor = resJson.get('cursor')
            if not cursor:
                break
        except Exception as e:
            logger.error(f"Error in _fetch_all_ads_from_page: {e}")
            break
            
    return ads[:limit]


def get_fanpage_ads(
    platform_ids: Union[str, List[str]],
    limit: Optional[int] = 50,
    country: Optional[str] = None,
    analyze_media: bool = True,
    target_file: Optional[str] = None,
    append_mode: bool = False,
    max_ads: Optional[int] = None,
    apply_filtering: bool = True
) -> Dict[str, Any]:
    """
    Unified fanpage tool: fetch all ads by page ID(s), filter, analyze media with Gemini, save to file.
    Full pipeline analogous to search_facebook_ads but using page IDs instead of keyword search.
    """
    key_manager.reset_all()

    if not platform_ids:
        return {"success": False, "message": "Missing platform_ids", "results": [], "count": 0}

    # Normalize: MCP may pass numeric IDs
    if isinstance(platform_ids, (int, float)):
        platform_ids = str(int(platform_ids))

    if isinstance(platform_ids, str):
        platform_list = [platform_ids.strip()]
    elif isinstance(platform_ids, list):
        platform_list = [str(int(pid) if isinstance(pid, (int, float)) else pid).strip() for pid in platform_ids if pid]
    else:
        return {"success": False, "message": "Invalid platform_ids type", "results": [], "count": 0}

    try:
        logging.info(f"Starting get_fanpage_ads for {len(platform_list)} page(s)")
        get_scrapecreators_api_key()

        fetch_limit = limit if limit else 50

        # Fetch ads from all pages
        all_ads = []
        for pid in platform_list:
            # Use our custom fetcher with filter_inactive=False
            page_ads = _fetch_all_ads_from_page(pid, fetch_limit, country)
            logging.info(f"Fetched {len(page_ads)} ads from page {pid} (including potentially processed/recent)")
            all_ads.extend(page_ads)

        if not all_ads:
            return {"success": True, "message": "No ads found for given platform IDs", "results": [], "count": 0}

        # Tag each ad with its source
        for ad in all_ads:
            ad['search_query'] = f"fanpage:{ad.get('page_id', 'unknown')}"

        # Group ads by ad_id to process all cards (variants) together
        groups = defaultdict(list)
        for ad in all_ads:
            groups[ad['ad_id']].append(ad)

        # Process each group in parallel (same pipeline as search_facebook_ads)
        from concurrent.futures import ThreadPoolExecutor
        final_processed_ads = []

        def process_single_group(group_data):
            ad_id, group = group_data
            if apply_filtering:
                group = [ad for ad in group if filter_ad(ad)]
                if not group:
                    return []
                group = detect_heuristics(group)
                if not group:
                    return []

            if analyze_media and group:
                group = analyze_ad_media_batch(group)
            return group

        with ThreadPoolExecutor(max_workers=10) as executor:
            group_list = list(groups.items())
            total_groups = len(group_list)
            print(f"DEBUG: [Fanpage] Processing {total_groups} groups in threads...", file=sys.stderr)

            processed_groups = list(executor.map(process_single_group, group_list))

            for idx, group in enumerate(processed_groups, 1):
                if idx % 10 == 0 or idx == total_groups:
                    print(f"PROGRESS: Processed {idx}/{total_groups} groups...", file=sys.stderr)
                for ad in group:
                    final_processed_ads.append(ad)

        # Format results
        formatted_ads = [convert_ad_to_file_format(ad) for ad in final_processed_ads]

        # Saving results
        saved_filepath = None

        if formatted_ads:
            try:
                if target_file:
                    filename_only = os.path.basename(target_file)
                else:
                    country_code = country if country else "ALL"
                    filename_only = f"fanpage_{country_code}.json"
                    append_mode = True

                final_ads_to_save = formatted_ads

                if append_mode:
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    results_dir_abs = os.path.join(current_dir, 'results')
                    check_path_abs = os.path.join(results_dir_abs, filename_only)

                    if os.path.exists(check_path_abs):
                        try:
                            existing_keys, existing_ads = load_existing_ads(check_path_abs)
                            new_unique_ads = filter_new_ads(formatted_ads, existing_keys, max_ads)
                            final_ads_to_save = existing_ads + new_unique_ads
                        except Exception as e:
                            logger.error(f"Append failed: {e}")

                saved_filepath = save_results(final_ads_to_save, filename_only)
                logging.info(f"Saved fanpage results to: {saved_filepath}")
            except Exception as save_err:
                logging.error(f"Saving failed: {save_err}")
                saved_filepath = f"ERROR_SAVING: {save_err}"

        return {
            "success": True,
            "message": f"Found {len(formatted_ads)} ads from fanpage(s). Saved to {saved_filepath}.",
            "results": formatted_ads,
            "count": len(formatted_ads),
            "total_found": len(all_ads),
            "saved_file": saved_filepath
        }

    except Exception as e:
        return {"success": False, "message": str(e), "results": [], "count": 0, "error": str(e)}


def analyze_ad_image(media_urls: Union[str, List[str]], brand_name: Optional[str] = None, ad_id: Optional[str] = None, ad_text: str = '') -> Dict[str, Any]:

    if isinstance(media_urls, str):
        media_url = media_urls
    elif isinstance(media_urls, list) and media_urls:
         media_url = media_urls[0]
    else:
        return {"success": False, "message": "Invalid media_urls"}

    if key_manager.all_exhausted:
         return {"success": False, "message": "All Gemini API keys exhausted", "error": "Quota exhausted"}

    try:
        # Check cache (skip if ad_text provided — context changes the analysis)
        if not ad_text:
            cached_data = image_cache.get_cached_image(media_url.strip())
            if cached_data and cached_data.get('analysis_results'):
                return {"success": True, "cached": True, "analysis": cached_data['analysis_results']}
        
        # Download
        response = requests.get(media_url.strip(), timeout=30)
        response.raise_for_status()
        
        image_bytes = response.content
        content_type = response.headers.get('content-type', '').lower()
        if not any(img_type in content_type for img_type in ['image/', 'jpeg', 'jpg', 'png', 'gif', 'webp']):
            return {"success": False, "message": f"Invalid content type: {content_type}", "error": "Invalid content type"}

        # Cache image
        image_cache.cache_image(
            url=media_url.strip(),
            image_data=image_bytes,
            content_type=content_type,
            brand_name=brand_name,
            ad_id=ad_id
        )

        image_data_b64 = base64.b64encode(image_bytes).decode('utf-8')

        if not GEMINI_AVAILABLE:
            return {"success": True, "cached": False, "image_data": image_data_b64, "message": "Gemini not available"}

        # Perform Analysis
        model = configure_gemini()
        
        ad_text_block = f"""ТЕКСТ ОБЪЯВЛЕНИЯ:
{ad_text[:2000]}

""" if ad_text else ""
        
        analysis_prompt = f"""{ad_text_block}Проанализируй изображение рекламы. Отвечай строго по этой структуре:

1. ОФФЕР: [Название бренда, препарата и форма (например, 'Grovi Gel гель'). Если препарат виден, но имя не читается — 'неизвестный [препарат/форма]'. Если это белая заглушка/white page — 'white'].
2. СОДЕРЖАНИЕ: [Максимально детально: ВЕСЬ ТЕКСТ на картинке, все люди и персонажи, их действия, позы, одежда, окружающая обстановка и все объекты].
"""
        raw_analysis = analyze_image_with_gemini(model, image_bytes, analysis_prompt, mime_type=content_type)
        
        analysis_result = {
            "raw_analysis": raw_analysis
        }
        
        # Update cache with analysis
        image_cache.update_analysis_results(media_url.strip(), analysis_result)
        
        return {
            "success": True, 
            "image_data": image_data_b64,
            "analysis_instructions": analysis_prompt,
            "cached": False,
            "analysis": analysis_result
        }

    except Exception as e:
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503']):
            key_manager.mark_key_dead()
        return {"success": False, "error": str(e)}

def analyze_ad_video(media_url: str, brand_name: str = None, ad_id: str = None, ad_text: str = None, api_key: Optional[str] = None, model: Optional[Any] = None) -> Dict[str, Any]:
    """Downloads a video and performs Gemini analysis with fixed key to avoid 403."""
    try:
        if not media_url:
            return {"success": False, "error": "No media_url provided"}
        
        # 1. Download to cache
        if key_manager.all_exhausted:
            return {"success": False, "message": "All Gemini API keys exhausted"}

        # Check cache (only if no ad_text context)
        if not ad_text:
            cached_data = media_cache.get_cached_media(media_url.strip(), media_type='video')
            if cached_data and cached_data.get('analysis_results'):
                return {"success": True, "cached": True, "analysis": cached_data['analysis_results']}
        
        # Download (if not cached file existed but no analysis)
        video_path = None
        cached_data = media_cache.get_cached_media(media_url.strip(), media_type='video')
        if cached_data:
            video_path = cached_data['file_path']
        else:
            resp = requests.get(media_url.strip(), timeout=60)
            resp.raise_for_status()
            content_type = resp.headers.get('content-type', '').lower()
            
            video_path = media_cache.cache_media(
                url=media_url.strip(),
                media_data=resp.content,
                content_type=content_type,
                media_type='video',
                brand_name=brand_name,
                ad_id=ad_id
            )
            
        if not GEMINI_AVAILABLE:
             return {"success": True, "cached": False, "message": "Gemini not available, video cached but not analyzed"}

        # Perform Analysis
        from services.gemini_service import configure_gemini, upload_video_to_gemini, analyze_video_with_gemini
        
        # Local model for upload/wait session
        if not model:
            # Important: Ensure the global config for THIS thread's upload session is correct
            import google.generativeai as genai
            genai.configure(api_key=api_key or get_gemini_api_key())
            model = genai.GenerativeModel('gemini-3.1-flash-lite')
        
        # Upload to Gemini (now REST-based and takes api_key)
        gemini_file = upload_video_to_gemini(video_path, api_key=api_key)
        
        ad_text_block = f"""ТЕКСТ ОБЪЯВЛЕНИЯ:
{ad_text[:2000]}

""" if ad_text else ""
        
        analysis_prompt = f"""{ad_text_block}Проанализируй видео рекламы. Отвечай по этой структуре сжато в информационном стиле без вступлений:

1. ОФФЕР: [Название бренда, препарата и форма. Если бренд не читается — 'неизвестный [препарат/форма]'. Если это видео-заглушка/white page — 'white'].
2. СЦЕНЫ: [ПОЛНОЕ описание визуала: кто в кадре, что делает, какие действия совершаются. Нужен ПОЛНЫЙ СКРИПТ БЕЗ ТЕКСТА на экране].
3. ФОРМАТ: [Длительность. Повторяется ли один статичный кадр дольше 60 секунд после динамических кадров?]
4. ХУК: [Описание первых 5 секунд: визуальная зацепка и основной смысловой посыл].
5. ОЗВУЧКА: [Если есть — переведи на русский и напиши ДОСЛОВНО ВЕСЬ ТЕКСТ озвучки (полная транскрипция всех слов)].
6. ЛЮДИ: [Возраст, пол, эмоции, национальность, одежда, детально что делают].
7. ПРИЗЫВ К ДЕЙСТВИЮ: [Текст CTA или "нет"].
"""
        # Call REST-based analysis passing the SAME key to avoid 403
        raw_analysis = analyze_video_with_gemini(model, gemini_file, analysis_prompt, api_key=api_key)
        
        # Cleanup remote file
        try:
            cleanup_gemini_file(gemini_file.name)
        except:
             pass

        analysis_result = {
            "raw_analysis": raw_analysis
        }
        
        # Update cache
        media_cache.update_analysis_results(media_url.strip(), analysis_result)
        
        return {
             "success": True,
             "cached": False,
             "analysis": analysis_result
        }

    except Exception as e:
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503']):
            key_manager.mark_key_dead()
        return {"success": False, "error": str(e)}

def analyze_ad_videos_batch(media_urls: List[str], brand_names: Optional[List[str]] = None, ad_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    # Simplified batch implementation iterating single calls for robustness in this manual version
    # or implementing true batch if critical. For now, let's just loop to ensure function exists and works.
    results = {}
    for i, url in enumerate(media_urls):
        bn = brand_names[i] if brand_names and i < len(brand_names) else None
        aid = ad_ids[i] if ad_ids and i < len(ad_ids) else None
        results[url] = analyze_ad_video(url, bn, aid)
    
    return {"success": True, "results": results}



def get_cache_stats() -> Dict[str, Any]:
    try:
        stats = media_cache.get_cache_stats()
        return {"success": True, "stats": stats}
    except Exception as e:
        return {"success": False, "error": str(e)}

def search_cached_media(brand_name: Optional[str] = None, has_people: Optional[bool] = None, color_contains: Optional[str] = None, media_type: Optional[str] = None, limit: Optional[int] = 20) -> Dict[str, Any]:
    try:
        results = media_cache.search_cached_media(brand_name, has_people, color_contains, media_type)
        return {"success": True, "results": results[:limit] if limit else results}
    except Exception as e:
        return {"success": False, "error": str(e)}

def cleanup_media_cache(max_age_days: Optional[int] = 30) -> Dict[str, Any]:
    try:
        media_cache.cleanup_old_cache(max_age_days=max_age_days or 30)
        return {"success": True, "message": "Cleanup done"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def clean_results_file(filename: str, overwrite: bool = True) -> Dict[str, Any]:
    """
    Reads a JSON results file, removes all ad cards where raw_analysis contains 'white',
    and saves the cleaned version.

    Args:
        filename: Name of the file in the results/ directory (e.g. 'DE_Prostatitis.json').
        overwrite: If True, overwrites the original file. If False, saves as '<name>_cleaned.json'.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(current_dir, 'results')
    filepath = os.path.join(results_dir, os.path.basename(filename))

    if not os.path.exists(filepath):
        return {"success": False, "error": f"File not found: {filepath}"}

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            ads = json.load(f)
    except Exception as e:
        return {"success": False, "error": f"Failed to read file: {e}"}

    if not isinstance(ads, list):
        return {"success": False, "error": "File content is not a JSON array"}

    total_before = len(ads)
    kept = []
    removed = 0

    for ad in ads:
        raw = None

        ma = ad.get('media_analysis')
        if isinstance(ma, dict):
            # analysis_error → keep without touching
            if 'analysis_error' in ma:
                kept.append(ad)
                continue

            # VIDEO: media_analysis.raw_analysis
            raw = ma.get('raw_analysis')

            # IMAGE (batch): media_analysis.image_analysis.raw_analysis
            if raw is None:
                img = ma.get('image_analysis')
                if isinstance(img, dict):
                    raw = img.get('raw_analysis')
        else:
            # No media_analysis at all → keep
            kept.append(ad)
            continue

        if raw is None:
            # No raw_analysis found → keep
            kept.append(ad)
            continue

        # Check if raw_analysis contains 'white' (case-insensitive)
        if 'white' in str(raw).lower():
            removed += 1
        else:
            kept.append(ad)

    # Determine output path
    if overwrite:
        out_path = filepath
    else:
        base, ext = os.path.splitext(os.path.basename(filename))
        out_path = os.path.join(results_dir, f"{base}_cleaned{ext}")

    try:
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(kept, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return {"success": False, "error": f"Failed to write file: {e}"}

    return {
        "success": True,
        "message": f"Cleaned '{os.path.basename(filename)}': removed {removed} white cards, kept {len(kept)}.",
        "total_before": total_before,
        "removed": removed,
        "kept": len(kept),
        "saved_to": out_path
    }


def retry_failed_gemini_analysis(json_file_path: str) -> Dict[str, Any]:
    """Retry Gemini analysis for ads that have failed or missing analysis in a local JSON file."""
    import json
    import time
    from pathlib import Path
    
    if not os.path.exists(json_file_path):
        return {"success": False, "error": f"File not found: {json_file_path}"}
        
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        results = data.get('results', []) if isinstance(data, dict) and 'results' in data else (data if isinstance(data, list) else [])
        if not results:
            return {"success": False, "error": "No ads found in JSON to retry"}
            
        failed_ads = []
        for ad in results:
            ma = ad.get('media_analysis')
            is_failed = False
            if not ma:
                is_failed = True
            elif 'analysis_error' in ma:
                is_failed = True
            elif 'raw_analysis' not in ma and not ma.get('image_analysis', {}).get('raw_analysis'):
                is_failed = True
            else:
                # Catch the fallback parsed strings like 'Analysis for card 2 not found in batch response.'
                raw_text = ma.get('raw_analysis') or ma.get('image_analysis', {}).get('raw_analysis', '')
                if "not found in batch response" in str(raw_text).lower():
                    is_failed = True
                
            if is_failed:
                failed_ads.append(ad)
                
        if not failed_ads:
            return {"success": True, "message": "No failed ads found. Nothing to retry."}
            
        logger.info(f"Found {len(failed_ads)} ads to retry Gemini analysis.")
        
        # Group ads by ad_id
        ad_groups = {}
        for ad in failed_ads:
            aid = ad.get('ad_id', 'unknown')
            if aid not in ad_groups:
                ad_groups[aid] = []
            ad_groups[aid].append(ad)
            
        for aid, group_ads in ad_groups.items():
            logger.info(f"Retrying analysis for Ad Group {aid} ({len(group_ads)} ads)")
            analyze_ad_media_batch(group_ads)
            time.sleep(2)
            
        # Overwrite JSON
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        return {
            "success": True,
            "message": f"Retried analysis for {len(failed_ads)} ads.",
            "retried_count": len(failed_ads)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

