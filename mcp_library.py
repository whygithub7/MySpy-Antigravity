
from services.scrapecreators_service import get_platform_id, get_ads, get_scrapecreators_api_key, get_platform_ids_batch, get_ads_batch, CreditExhaustedException, RateLimitException, search_ads_by_keyword
from services.media_cache_service import media_cache, image_cache
from services.gemini_service import configure_gemini, upload_video_to_gemini, analyze_video_with_gemini, cleanup_gemini_file, analyze_videos_batch_with_gemini, upload_videos_batch_to_gemini, cleanup_gemini_files_batch, get_gemini_api_key, analyze_image_with_gemini
from typing import Dict, Any, List, Optional, Union
from collections import defaultdict
import requests
import base64
import os
import json
import logging
from dotenv import load_dotenv
import sys
from datetime import datetime
import re

# Configure logging to stderr
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), '.env')
if not os.path.exists(env_path):
    # Try parent directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(env_path)

# Setup logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("mcp_library")

# Global variable for Gemini quota tracking
GEMINI_QUOTA_EXHAUSTED = False

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
    'facebook', 'fb.me', 'fb.com', 'instagram', 'whatsapp', 'wa.me', 'messenger',
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
    '/shop/', '/store/', '/marketplace/', '/cart/', '/checkout/',
    '/psycholog', '/therapy/', '/counseling/', '/hypnosis/',
    '/fitness/', '/gym/', '/workout/',
    '/product/', '/item/'
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


def check_if_excluded_content_via_gemini(analysis_text: str) -> bool:
    """
    Uses Gemini to contextually analyze text and determine if content should be excluded.
    """
    global GEMINI_QUOTA_EXHAUSTED
    
    if not GEMINI_AVAILABLE or not analysis_text or GEMINI_QUOTA_EXHAUSTED:
        return False
    
    try:
        model = configure_gemini()
        
        context_prompt = """Analyze the following Facebook ad content analysis and determine if this ad promotes:

1. Online courses, training programs, educational courses (NOT medical treatment courses)
2. Reading applications, e-book apps, or similar applications
3. Educational platforms (Udemy, Coursera, Hotmart, Teachable, etc.)
4. Marketplaces (Amazon, eBay, AliExpress, Mercado Libre, general e-commerce platforms)
5. Psychology services (психология, психотерапия, консультации психолога, психологические тренинги, коучинг)
6. Hypnosis services (гипноз, гипнотерапия, hypnotherapy)
7. Sports, fitness, or gym (фитнес, спорт, тренажерный зал, спортивное питание, gym, workout equipment, athletic training)
8. General wellness or self-help not related to specific medical treatment (мотивация, личностный рост, саморазвитие)
9. Information products (инфо-продукты, инфо-курсы, вебинары, мастер-классы)
10. Physical consumer goods NOT related to medical treatment (watches, jewelry, clothing, shoes, general electronics, gadgets, automotive, real estate)

IMPORTANT CONTEXT:
- "Course of treatment" or "medical course" (курс лечения) = ACCEPTABLE (medical treatment)
- "Training course" or "online course" = EXCLUDE (educational content)
- Medical devices like tonometer or glucometer  in medical product ads = ACCEPTABLE
- Medical supplements, vitamins, medicines for specific conditions = ACCEPTABLE
- Reading apps, e-book apps = EXCLUDE
- Sports equipment, gym memberships, fitness coaching = EXCLUDE
- Psychological counseling, therapy sessions, mental wellness coaching = EXCLUDE
- Hypnosis for any purpose = EXCLUDE
- General marketplace ads (selling variety of products) = EXCLUDE
- Watches (smart or classic), Jewelry, Clothing, Fashion = EXCLUDE

Respond with ONLY one word: "EXCLUDE" if the ad should be excluded, or "KEEP" if it should be kept.

Content analysis:
{analysis_text}
""".format(analysis_text=analysis_text[:8000])
        
        response = model.generate_content(context_prompt)
        
        if response.text:
            result = response.text.strip().upper()
            return "EXCLUDE" in result or result == "EXCLUDE"
        
        return False
        
    except Exception as e:
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
            if not GEMINI_QUOTA_EXHAUSTED:
                GEMINI_QUOTA_EXHAUSTED = True
                if 'leaked' in error_str or '403' in error_str:
                    print(f"Warning: Gemini API key blocked or lost: {e}", file=sys.stderr)
                else:
                    print(f"Warning: Gemini quota exhausted", file=sys.stderr)
            return False
        print(f"Warning: Gemini context check failed: {e}", file=sys.stderr)
        return False


def filter_ad(ad: Dict[str, Any], use_media_analysis: bool = False) -> bool:
    """Basic ad filtration logic."""
    if not ad.get('has_external_links'):
        return False
    
    external_urls = ad.get('external_urls', [])
    if not external_urls:
        return False
    
    primary_url = external_urls[0].get('full_url', '')
    if not primary_url:
        return False
    
    domain = external_urls[0].get('domain', '')
    if is_excluded_domain(domain):
        return False
    
    if is_excluded_url(primary_url):
        return False
    
    body_text = ad.get('body', '') or ''
    title_text = ad.get('title', '') or ''
    
    combined_text_length = len(body_text) + len(title_text)
    if combined_text_length > 4000:
        return False
        
    # Check text content via Gemini
    text_to_check = f"{title_text}\n{body_text}"
    if text_to_check.strip():
        if check_if_excluded_content_via_gemini(text_to_check):
            return False
    
    if use_media_analysis:
        media_analysis = ad.get('media_analysis', {})
        
        image_analysis = media_analysis.get('image_analysis')
        if image_analysis is not None:
            if isinstance(image_analysis, dict) and 'raw_analysis' in image_analysis:
                analysis_text = image_analysis.get('raw_analysis', '')
                if analysis_text:
                    if check_if_excluded_content_via_gemini(analysis_text):
                        return False
        
        video_analysis = media_analysis.get('video_analysis')
        if video_analysis is not None:
            if isinstance(video_analysis, dict) and 'raw_analysis' in video_analysis:
                analysis_text = video_analysis.get('raw_analysis', '')
                if analysis_text:
                    if check_if_excluded_content_via_gemini(analysis_text):
                        return False
    
    return True


def analyze_media_func(ad: Dict[str, Any]) -> Dict[str, Any]:
    """Analyzes ad media."""
    global GEMINI_QUOTA_EXHAUSTED
    
    media_type = ad.get('media_type', '')
    media_url = ad.get('media_url', '')
    ad_id = ad.get('ad_id', '')
    
    analysis_result = {
        'image_analysis': None,
        'video_analysis': None,
        'analysis_error': None
    }
    
    if GEMINI_QUOTA_EXHAUSTED:
        analysis_result['analysis_error'] = "Analysis skipped: Gemini quota previously exhausted in this session."
        return analysis_result

    if not media_url:
        analysis_result['analysis_error'] = "No media URL provided."
        return analysis_result
    
    try:
        if media_type.upper() == 'IMAGE':
            result = analyze_ad_image(media_urls=media_url, brand_name=None, ad_id=ad_id)
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
                    if not GEMINI_QUOTA_EXHAUSTED:
                        GEMINI_QUOTA_EXHAUSTED = True
        
        elif media_type.upper() == 'VIDEO':
            result = analyze_ad_video(media_url=media_url, brand_name=None, ad_id=ad_id)
            if result.get('success'):
                analysis_result['video_analysis'] = result.get('analysis', {})
            else:
                error = result.get('error', 'Unknown error')
                analysis_result['analysis_error'] = error
                error_str = str(error).lower()
                if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
                    if not GEMINI_QUOTA_EXHAUSTED:
                        GEMINI_QUOTA_EXHAUSTED = True
        
        elif media_type.upper() == 'DCO':
            result = analyze_ad_image(media_urls=media_url, brand_name=None, ad_id=ad_id)
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
                    if not GEMINI_QUOTA_EXHAUSTED:
                        GEMINI_QUOTA_EXHAUSTED = True
    
    except Exception as e:
        error_str = str(e).lower()
        analysis_result['analysis_error'] = str(e)
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503']):
            GEMINI_QUOTA_EXHAUSTED = True
    
    return analysis_result


def deduplicate_ads(ads_by_url: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Deduplicates ads by external URL."""
    deduplicated = []
    
    for url, ads_list in ads_by_url.items():
        if len(ads_list) == 1:
            deduplicated.append(ads_list[0])
        elif len(ads_list) == 2:
            deduplicated.extend(ads_list)
        else:
            first_ad = ads_list[0].copy()
            first_ad['url_occurrences'] = len(ads_list)
            deduplicated.append(first_ad)
    
    return deduplicated


def convert_ad_to_file_format(ad: dict) -> dict:
    """Converts ad to file format."""
    ad_data = {
        'ad_id': ad.get('ad_id'),
        'ad_text': ad.get('body'),
        'external_urls': [u.get('full_url') if isinstance(u, dict) else u for u in ad.get('external_urls', [])],
        'fanpage_url': f"https://www.facebook.com/{ad.get('page_id', '')}" if ad.get('page_id') else None,
        'ad_url': f"https://www.facebook.com/ads/library/?id={ad.get('ad_id')}" if ad.get('ad_id') else None,
        'start_date': ad.get('start_date'),
        'end_date': ad.get('end_date'),
        'media_type': ad.get('media_type'),
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
    """Loads existing ads from file."""
    if not os.path.exists(filepath):
        return set(), []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        existing_ads = json.load(f)
    
    existing_urls = set()
    for ad in existing_ads:
        external_urls = ad.get('external_urls', [])
        if external_urls:
            for url in external_urls:
                if isinstance(url, str) and url.strip():
                    existing_urls.add(url.strip())
                elif isinstance(url, dict) and url.get('full_url'):
                    existing_urls.add(url.get('full_url').strip())
    
    return existing_urls, existing_ads


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


def filter_new_ads(ads: list, existing_urls: set, max_ads: int = None) -> list:
    """Filters ads keeping only new ones."""
    new_ads = []
    for ad in ads:
        external_urls = ad.get('external_urls', [])
        if external_urls:
            if isinstance(external_urls[0], dict):
                primary_url = external_urls[0].get('full_url', '')
            else:
                primary_url = external_urls[0]
            
            if primary_url and primary_url.strip() and primary_url.strip() not in existing_urls:
                new_ads.append(ad)
                existing_urls.add(primary_url.strip())
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
    apply_filtering: bool = True
) -> Dict[str, Any]:
    """
    Unified function to search for Facebook ads with optional media analysis, filtering, and file saving.
    
    Args:
        query: Search keywords.
        limit: Max ads to fetch from API.
        country: Country code.
        active_status: "ACTIVE", "ALL", etc.
        media_type: "ALL", "IMAGE", "VIDEO".
        analyze_media: Enable Gemini analysis (default: True).
        target_file: Filename to save/append results to (e.g., "results.json").
        append_mode: If True, append to target_file instead of overwriting.
        max_ads: Limit saved ads count (if filtering reduces count).
        apply_filtering: Enable domain/content logic filtering (default: True).
    """
    global GEMINI_QUOTA_EXHAUSTED
    # Reset quota flag for each new search session to allow recovery if credits were added
    GEMINI_QUOTA_EXHAUSTED = False

    if not query or not query.strip():
        return {"success": False, "message": "Missing query", "results": [], "count": 0}

    try:
        logging.info(f"Starting search_facebook_ads V3 for query: {query}")
        get_scrapecreators_api_key()
        
        # Use simple search logic provided by service
        # Force min 100 to optimize credit usage (1 credit = up to 100 ads)
        # If max_ads is set and higher than limit, use max_ads as the fetch limit
        req_limit = limit or 200
        if max_ads and max_ads > req_limit:
            req_limit = max_ads

        fetch_limit = 100 if req_limit < 100 else req_limit
        
        logging.info(f"Fetching {fetch_limit} ads (Requested {req_limit})")
        
        ads = search_ads_by_keyword(
            query=query,
            limit=fetch_limit,
            country=country,
            active_status=active_status,
            media_type=media_type,
            trim=False
        )
        
        if not ads:
             return {"success": True, "message": f"No ads found for query: {query}", "results": [], "count": 0}

        # Add search_query to each ad (fixes null issue)
        for ad in ads:
            ad['search_query'] = query

        # Filtering and Analysis Pipeline
        filtered_ads = []
        ads_by_url = defaultdict(list)
        
        # Determine actual processing limit
        # If max_ads is set, we might stop early? 
        # But we need to filter first. 
        # Lets process all fetched ads.
        
        for ad in ads:
            # 1. Base Filtering (Optional)
            if apply_filtering:
                if not filter_ad(ad):
                     continue
            
            # 2. Media Analysis (Optional)
            if analyze_media:
                media_analysis = analyze_media_func(ad)
                ad['media_analysis'] = media_analysis
                
                # 3. Content Filtering based on Analysis (Optional)
                if apply_filtering:
                    if not filter_ad(ad, use_media_analysis=True):
                        continue
            
            filtered_ads.append(ad)
            
            if max_ads and len(filtered_ads) >= max_ads:
                break
        
        # Deduplication and Grouping
        for ad in filtered_ads:
            external_urls = ad.get('external_urls', [])
            if external_urls:
                # Handle both dict and string external urls
                if isinstance(external_urls[0], dict):
                     primary_url = external_urls[0].get('full_url', '')
                else:
                     primary_url = external_urls[0]

                if primary_url:
                    ads_by_url[primary_url].append(ad)
        
        deduplicated_ads = deduplicate_ads(ads_by_url)
        formatted_ads = [convert_ad_to_file_format(ad) for ad in deduplicated_ads]
        
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
                             existing_urls, existing_ads = load_existing_ads(check_path_abs)
                             new_unique_ads = filter_new_ads(formatted_ads, existing_urls, max_ads)
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


def get_meta_ads(platform_ids: Union[str, List[str]], limit: Optional[int] = 50, country: Optional[str] = None, trim: Optional[bool] = True) -> Dict[str, Any]:
    # Simplified validation
    if isinstance(platform_ids, str):
        platform_list = [platform_ids.strip()]
        is_single = True
    else:
        platform_list = platform_ids
        is_single = False
        
    try:
        get_scrapecreators_api_key()
        if is_single:
            ads = get_ads(platform_list[0], limit or 50, country, trim)
            return {"success": True, "message": f"Found {len(ads)} ads", "results": ads, "count": len(ads)}
        else:
            batch_results = get_ads_batch(platform_list, limit or 50, country, trim)
            total = sum(len(a) for a in batch_results.values())
            return {"success": True, "message": f"Found {total} ads", "results": batch_results, "count": total}
    except Exception as e:
        return {"success": False, "message": str(e), "results": [], "count": 0, "error": str(e)}


def get_meta_ads_external_only(platform_ids: Union[str, List[str]], limit: Optional[int] = 50, country: Optional[str] = None, min_results: Optional[int] = None) -> Dict[str, Any]:
     # Reuse get_meta_ads logic but filter for external links
    if isinstance(platform_ids, str):
        platform_list = [platform_ids.strip()]
        is_single = True
    else:
        platform_list = platform_ids
        is_single = False

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



def analyze_ad_image(media_urls: Union[str, List[str]], brand_name: Optional[str] = None, ad_id: Optional[str] = None) -> Dict[str, Any]:
    global GEMINI_QUOTA_EXHAUSTED
    if isinstance(media_urls, str):
        media_url = media_urls
    elif isinstance(media_urls, list) and media_urls:
         media_url = media_urls[0]
    else:
        return {"success": False, "message": "Invalid media_urls"}

    if GEMINI_QUOTA_EXHAUSTED:
         return {"success": False, "message": "Gemini quota exhausted", "error": "Quota exhausted"}

    try:
        # Check cache
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
        
        analysis_prompt = """
Проанализируй это изображение из рекламы Facebook и извлеки ВСЮ фактическую информацию. ОТВЕЧАЙ СТРОГО НА РУССКОМ ЯЗЫКЕ без воды.

**Общее визуальное описание:**
- Полное описание того, что показано на изображении

**Текстовые элементы:**
- Определи и транскрибируй ВЕСЬ текст, присутствующий на изображении
- Классифицируй каждый текстовый элемент как:
  * "Заголовок-хук"
  * "Ценностное предложение"
  * "Призыв к действию (CTA)"
  * "Реферальная программа"
  * "Отказ от ответственности"
  * "Название бренда"
  * "Другое"

**Описание людей:**
- Для каждого видимого человека: возрастной диапазон, пол, внешность, одежда, поза, выражение лица, обстановка

**Элементы бренда:**
- Присутствующие логотипы
- Снимки продуктов
- Цвета бренда

**Композиция и макет:**
- Структура макета
- Визуальная иерархия
- Позиционирование элементов
- Наложение текста vs отдельные текстовые области

**Цвета и визуальный стиль:**
- Доминирующие цвета
- Цвет/тип фона
- Стиль фотографии

**Технические и индикаторы целевой аудитории:**
- Формат изображения
- Читаемость текста
- Визуальные подсказки о целевой аудитории
- Детали обстановки/окружения

**Сообщение и тема:**
- Какую историю или сообщение передает визуал
- Эмоциональный тон и настроение
- Индикаторы маркетинговой стратегии

Извлеки ВСЮ эту информацию комплексно.
"""
        raw_analysis = analyze_image_with_gemini(model, image_bytes, analysis_prompt, mime_type=content_type)
        
        analysis_result = {
            "raw_analysis": raw_analysis
            # In a full implementation we might parse this structured text into JSON fields, 
            # but raw_analysis is what search_ads.py uses for filtering.
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
            GEMINI_QUOTA_EXHAUSTED = True
        return {"success": False, "error": str(e)}

def analyze_ad_video(media_url: str, brand_name: Optional[str] = None, ad_id: Optional[str] = None) -> Dict[str, Any]:
    global GEMINI_QUOTA_EXHAUSTED
    if not media_url or not media_url.strip():
        return {"success": False, "message": "Missing media URL"}

    if GEMINI_QUOTA_EXHAUSTED:
        return {"success": False, "message": "Gemini quota exhausted"}

    try:
        # Check cache
        cached_data = media_cache.get_cached_media(media_url.strip(), media_type='video')
        if cached_data and cached_data.get('analysis_results'):
            return {"success": True, "cached": True, "analysis": cached_data['analysis_results']}
        
        # Download (if not cached file existed but no analysis)
        video_path = None
        if cached_data:
            video_path = cached_data['file_path']
        else:
            response = requests.get(media_url.strip(), timeout=60)
            response.raise_for_status()
            content_type = response.headers.get('content-type', '').lower()
            
            video_path = media_cache.cache_media(
                url=media_url.strip(),
                media_data=response.content,
                content_type=content_type,
                media_type='video',
                brand_name=brand_name,
                ad_id=ad_id
            )
            
        if not GEMINI_AVAILABLE:
             return {"success": True, "cached": False, "message": "Gemini not available, video cached but not analyzed"}

        # Perform Analysis
        model = configure_gemini()
        
        # Upload to Gemini
        gemini_file = upload_video_to_gemini(video_path)
        
        analysis_prompt = """
Проанализируй это видео из рекламы Facebook и предоставь подробный структурированный анализ в следующем формате. ОТВЕЧАЙ СТРОГО НА РУССКОМ ЯЗЫКЕ.

**АНАЛИЗ СЦЕН:**
Проанализируй видео по сценам.

**ОБЩИЙ АНАЛИЗ ВИДЕО:**

**Формат рекламы:**
- Формат, соотношение сторон, длительность

**Примечательные ракурсы:**
- Ракурсы камеры

**Общее сообщение:**
- Основное сообщение, целевая аудитория

**Анализ хука:**
- Тип хука, описание

**Аудио и Музыка:**
- Транскрипция, стиль музыки

**Элементы бренда:**
- Логотипы, продукты

Выведи полный подробный отчет.
"""
        raw_analysis = analyze_video_with_gemini(model, gemini_file, analysis_prompt)
        
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
            GEMINI_QUOTA_EXHAUSTED = True
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
