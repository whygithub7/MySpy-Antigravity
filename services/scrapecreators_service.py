import requests
import sys
import os
import logging
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from urllib.parse import urlparse, parse_qs

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SEARCH_API_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/search/companies"
SEARCH_ADS_API_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/search/ads"
ADS_API_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"


SCRAPECREATORS_API_KEY = None

# --- Custom Exceptions ---

class CreditExhaustedException(Exception):
    """Raised when ScrapeCreators API credits are exhausted."""
    def __init__(self, message: str, credits_remaining: int = 0, topup_url: str = "https://scrapecreators.com/dashboard"):
        self.credits_remaining = credits_remaining
        self.topup_url = topup_url
        super().__init__(message)

class RateLimitException(Exception):
    """Raised when ScrapeCreators API rate limit is exceeded."""
    def __init__(self, message: str, retry_after: int = None):
        self.retry_after = retry_after
        super().__init__(message)

# --- Helper Functions ---

def check_credit_status(response: requests.Response) -> Optional[Dict[str, Any]]:
    """
    Check response for credit-related information and errors.
    
    Args:
        response: HTTP response from ScrapeCreators API
        
    Returns:
        Dictionary with credit info if available, None otherwise
        
    Raises:
        CreditExhaustedException: If credits are exhausted
        RateLimitException: If rate limit is exceeded
    """
    # Check for credit exhaustion status codes
    if response.status_code == 402:  # Payment Required
        raise CreditExhaustedException(
            "ScrapeCreators API credits exhausted. Please top up your account to continue.",
            credits_remaining=0
        )
    elif response.status_code == 429:  # Too Many Requests
        retry_after = response.headers.get('retry-after')
        raise RateLimitException(
            "ScrapeCreators API rate limit exceeded. Please wait before making more requests.",
            retry_after=int(retry_after) if retry_after else None
        )
    elif response.status_code == 403:  # Forbidden - could indicate credit issues
        # Check if it's credit-related
        try:
            error_data = response.json()
            if 'credit' in str(error_data).lower() or 'quota' in str(error_data).lower():
                raise CreditExhaustedException(
                    "ScrapeCreators API access denied. This may indicate insufficient credits.",
                    credits_remaining=0
                )
        except:
            pass  # Not JSON or not credit-related
    
    # Extract credit information from headers if available
    credit_info = {}
    headers = response.headers
    
    # Common header names for credit information
    for header_name in ['x-credits-remaining', 'x-credit-remaining', 'credits-remaining']:
        if header_name in headers:
            try:
                credit_info['credits_remaining'] = int(headers[header_name])
            except ValueError:
                pass
    
    for header_name in ['x-credit-cost', 'credit-cost', 'x-credits-used']:
        if header_name in headers:
            try:
                credit_info['credit_cost'] = int(headers[header_name])
            except ValueError:
                pass
    
    return credit_info if credit_info else None

def get_scrapecreators_api_key() -> str:
    """
    Get ScrapeCreators API key from command line arguments or environment variable.
    Caches the key in memory after first read.
    Priority: command line argument > environment variable

    Returns:
        str: The ScrapeCreators API key.

    Raises:
        Exception: If no key is provided in command line arguments or environment.
    """
    global SCRAPECREATORS_API_KEY
    global SCRAPECREATORS_API_KEY
    # Always reload from env to support key changes
    if True: # Modified: Force check to allow dynamic updates
        # Try command line argument first
        if "--scrapecreators-api-key" in sys.argv:
            token_index = sys.argv.index("--scrapecreators-api-key") + 1
            if token_index < len(sys.argv):
                SCRAPECREATORS_API_KEY = sys.argv[token_index]
                logger.info(f"Using ScrapeCreators API key from command line arguments")
            else:
                raise Exception("--scrapecreators-api-key argument provided but no key value followed it")
        # Try environment variable
        elif os.getenv("SCRAPECREATORS_API_KEY"):
            SCRAPECREATORS_API_KEY = os.getenv("SCRAPECREATORS_API_KEY")
            logger.info(f"Using ScrapeCreators API key from environment variable")
        else:
            raise Exception("ScrapeCreators API key must be provided via '--scrapecreators-api-key' command line argument or 'SCRAPECREATORS_API_KEY' environment variable")

    return SCRAPECREATORS_API_KEY


def get_platform_id(brand_name: str) -> Dict[str, str]:
    """
    Get the Meta Platform ID for a given brand name.
    
    Args:
        brand_name: The name of the company or brand to search for.
    
    Returns:
        Dictionary mapping brand names to their Meta Platform IDs.
    
    Raises:
        requests.RequestException: If the API request fails.
        Exception: For other errors.
    """
    api_key = get_scrapecreators_api_key()
    
    response = requests.get(
        SEARCH_API_URL,
        headers={"x-api-key": api_key},
        params={
            "query": brand_name,
        },
        timeout=30  # Add timeout for better error handling
    )
    
    # Check for credit-related issues before raising for status
    credit_info = check_credit_status(response)
    response.raise_for_status()
    content = response.json()
    logger.info(f"Search response for '{brand_name}': {len(content.get('searchResults', []))} results found")
    
    options = {}
    for result in content.get("searchResults", []):
        name = result.get("name")
        page_id = result.get("page_id")
        if name and page_id:
            options[name] = page_id
    
    return options


def get_ads(
    page_id: str, 
    limit: int = 50,
    country: Optional[str] = None,
    trim: bool = True
) -> List[Dict[str, Any]]:
    """
    Get ads for a specific page ID with pagination support.
    
    Args:
        page_id: The Meta Platform ID for the brand.
        limit: Maximum number of ads to retrieve.
        country: Optional country code to filter ads (e.g., "US", "CA").
        trim: Whether to trim the response to essential fields only.
    
    Returns:
        List of ad objects with details.
    
    Raises:
        requests.RequestException: If the API request fails.
        Exception: For other errors.
    """
    api_key = get_scrapecreators_api_key()
    cursor = None
    headers = {
        "x-api-key": api_key
    }
    params = {
        "pageId": page_id,
        "limit": min(limit, 1500)  # Documentation suggests ~1500 limit for GET requests
    }
    
    # Add optional parameters if provided
    if country:
        params["country"] = country.upper()
    if trim:
        params["trim"] = "true"

    ads = []
    total_requests = 0
    max_requests = 10
    
    while len(ads) < limit and total_requests < max_requests:
        if cursor:
            params['cursor'] = cursor
        
        try:
            response = requests.get(
                ADS_API_URL, 
                headers=headers, 
                params=params,
                timeout=30
            )
            total_requests += 1
            
            # Check for credit-related issues
            try:
                credit_info = check_credit_status(response)
            except (CreditExhaustedException, RateLimitException):
                # Re-raise credit/rate limit exceptions to be handled by caller
                raise
            
            if response.status_code != 200:
                logger.error(f"Error getting FB ads for page {page_id}: {response.status_code} {response.text}")
                break
                
            resJson = response.json()
            logger.info(f"Retrieved {len(resJson.get('results', []))} ads from API (request {total_requests})")
            
            res_ads = parse_fb_ads(resJson, trim)
            if len(res_ads) == 0:
                logger.info("No more ads found, stopping pagination")
                break
                
            ads.extend(res_ads)
            
            # Get cursor for next page
            cursor = resJson.get('cursor')
            if not cursor:
                logger.info("No cursor found, reached end of results")
                break
                
        except requests.RequestException as e:
            logger.error(f"Network error while fetching ads: {str(e)}")
            break
        except Exception as e:
            logger.error(f"Error processing ads response: {str(e)}")
            break

    # Trim to requested limit
    return ads[:limit]


def search_ads_by_keyword(
    query: str,
    limit: int = 100,
    country: Optional[str] = None,
    ad_type: str = "ALL",
    media_type: str = "ALL",
    active_status: str = "ACTIVE",
    trim: bool = True,
    cursor: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Search for ads by keyword.
    
    Args:
        query: The keyword(s) to search for.
        limit: Maximum number of ads to retrieve.
        country: Optional country code (e.g., "US", "CA").
        ad_type: Type of ad ("ALL", "POLITICAL_AND_ISSUE_ADS").
        media_type: Type of media ("ALL", "IMAGE", "VIDEO").
        active_status: Status of ads ("ACTIVE", "ALL", "INACTIVE").
        trim: Whether to trim the response.
        cursor: Optional cursor to start/resume search from.
        start_date: Filter ads that started after this date (YYYY-MM-DD).
        end_date: Filter ads that started before this date (YYYY-MM-DD).
                  Tip: to find ads active > N days, set end_date = today - N days.
    
    Returns:
        List of ad objects.
    """
    api_key = get_scrapecreators_api_key()
    headers = {
        "x-api-key": api_key,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    params = {
        "query": query,
        "limit": min(limit, 100),
        "media_type": media_type,
        "active_status": active_status
    }
    
    if ad_type and ad_type.upper() != "ALL":
        params["ad_type"] = ad_type
    
    if country:
        params["country"] = country.upper()
    if trim:
        params["trim"] = "true"
    if start_date:
        params["start_date"] = start_date

    ads = []
    
    try:
        current_timeout = 120
        # Always request the full limit (default 100) in one go
        params['limit'] = limit
        
        logger.info(f"Searching ScrapeCreators for query '{query}' (limit={limit})")
        response = requests.get(
            SEARCH_ADS_API_URL, 
            headers=headers, 
            params=params,
            timeout=current_timeout
        )
        
        try:
            check_credit_status(response)
        except (CreditExhaustedException, RateLimitException):
            raise
        
        if response.status_code != 200:
            logger.error(f"Error {response.status_code}: {response.text[:200]}")
            return []
            
        resJson = response.json()
        search_results = resJson.get('searchResults', [])
        raw_count = len(search_results)
        logger.info(f"Retrieved {raw_count} raw ads")
        
        if raw_count == 0:
            return []
        
        # Transform and parse ALL variations/media objects
        transformed_response = {'results': search_results}
        res_ads = parse_fb_ads(transformed_response, trim, filter_inactive=False)
        
        if res_ads:
            ads.extend(res_ads)
        
        # Rule: if less than 100 returned the first time, don't demand other ads (pagination)
        # However, if we got exactly 100 or more, we are satisfied.
        # We don't use cursor/pagination here because of the 'one time' rule for < 100.
            
    except (CreditExhaustedException, RateLimitException):
        raise
    except Exception as e:
        logger.error(f"Error in search: {str(e)}")

    logger.info(f"Finished search. Total media objects captured: {len(ads)}")
    return ads[:limit]


def get_platform_ids_batch(brand_names: List[str]) -> Dict[str, Dict[str, str]]:
    """
    Get Meta Platform IDs for multiple brand names with deduplication.
    
    Args:
        brand_names: List of company or brand names to search for.
    
    Returns:
        Dictionary mapping brand names to their platform ID results.
        Format: {brand_name: {platform_name: platform_id, ...}, ...}
    
    Raises:
        CreditExhaustedException: If API credits are exhausted
        RateLimitException: If rate limit is exceeded
        requests.RequestException: If API requests fail
    """
    # Deduplicate brand names while preserving order
    unique_brands = list(dict.fromkeys(brand_names))
    results = {}
    
    logger.info(f"Batch processing {len(unique_brands)} unique brands from {len(brand_names)} requested")
    
    for brand_name in unique_brands:
        try:
            platform_ids = get_platform_id(brand_name)
            results[brand_name] = platform_ids
            logger.info(f"Successfully retrieved platform IDs for '{brand_name}': {len(platform_ids)} found")
        except (CreditExhaustedException, RateLimitException):
            # Re-raise credit/rate limit exceptions immediately
            raise
        except Exception as e:
            logger.error(f"Failed to get platform IDs for '{brand_name}': {str(e)}")
            results[brand_name] = {}
    
    return results


def get_ads_batch(platform_ids: List[str], limit: int = 50, country: Optional[str] = None, trim: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get ads for multiple platform IDs with deduplication.
    
    Args:
        platform_ids: List of Meta Platform IDs.
        limit: Maximum number of ads to retrieve per platform ID.
        country: Optional country code to filter ads.
        trim: Whether to trim the response to essential fields only.
    
    Returns:
        Dictionary mapping platform IDs to their ad results.
        Format: {platform_id: [ad_objects...], ...}
    
    Raises:
        CreditExhaustedException: If API credits are exhausted
        RateLimitException: If rate limit is exceeded
        requests.RequestException: If API requests fail
    """
    # Deduplicate platform IDs while preserving order
    unique_platform_ids = list(dict.fromkeys(platform_ids))
    results = {}
    
    logger.info(f"Batch processing {len(unique_platform_ids)} unique platform IDs from {len(platform_ids)} requested")
    
    for platform_id in unique_platform_ids:
        try:
            ads = get_ads(platform_id, limit, country, trim)
            results[platform_id] = ads
            logger.info(f"Successfully retrieved {len(ads)} ads for platform ID '{platform_id}'")
        except (CreditExhaustedException, RateLimitException):
            # Re-raise credit/rate limit exceptions immediately
            raise
        except Exception as e:
            logger.error(f"Failed to get ads for platform ID '{platform_id}': {str(e)}")
            results[platform_id] = []
    
    return results


def parse_url_utm_params(url: str) -> Dict[str, Any]:
    """
    Parse UTM parameters and other query parameters from a URL.
    
    Args:
        url: Full URL string with query parameters.
    
    Returns:
        Dictionary containing:
        - full_url: Complete URL with all parameters
        - base_url: URL without query parameters
        - domain: Domain name (lowercase)
        - utm_params: Dictionary of UTM parameters (utm_source, utm_medium, etc.)
        - all_params: Dictionary of all query parameters
        - is_internal: Boolean indicating if domain is Meta/Google internal
    """
    if not url or not isinstance(url, str):
        return None
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower() if parsed.netloc else None
        
        # Parse all query parameters
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        # Convert lists to single values (parse_qs returns lists)
        all_params = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
        
        # Extract UTM parameters specifically
        utm_params = {}
        utm_keys = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 
                   'utm_id', 'utm_source_platform', 'fbclid', 'gclid']
        for key in utm_keys:
            if key in all_params:
                utm_params[key] = all_params[key]
        
        # Check if internal Meta/Google domain
        # Указываем только базовые домены, проверка по окончанию покрывает все поддомены автоматически
        internal_base_domains = {
            # Meta/Facebook
            'facebook.com', 'fb.com', 'fbcdn.net', 'facebook.net',
            'instagram.com', 'ig.com',
            'messenger.com',
            'whatsapp.com', 'wa.me', 'whatsapp.net',
            'meta.com',
            'oculus.com',
            'threads.net',
            # Google
            'google.com', 'googleapis.com', 'googleusercontent.com', 'googletagmanager.com',
            'youtube.com', 'youtu.be', 'ytimg.com',
            'doubleclick.net', 'googleadservices.com', 'googlesyndication.com',
            'gmail.com', 'googlemail.com',
            'blogger.com', 'blogspot.com',
            'googleads.com', 'google-analytics.com', 'googleadwords.com'
        }
        # Проверяем окончание домена - это автоматически покрывает все поддомены
        is_internal = domain and any(domain == d or domain.endswith(f'.{d}') for d in internal_base_domains)
        
        return {
            'full_url': url,
            'base_url': f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.netloc else url.split('?')[0],
            'domain': domain,
            'utm_params': utm_params,
            'all_params': all_params,
            'is_internal': is_internal,
            'has_utm': len(utm_params) > 0
        }
    except Exception as e:
        logger.warning(f"Failed to parse URL {url}: {str(e)}")
        return {
            'full_url': url,
            'base_url': url.split('?')[0] if '?' in url else url,
            'domain': None,
            'utm_params': {},
            'all_params': {},
            'is_internal': False,
            'has_utm': False,
            'parse_error': str(e)
        }


def extract_all_urls_from_snapshot(snapshot: Dict[str, Any]) -> List[str]:
    """
    Extract all URLs from Facebook ad snapshot data.
    Searches in common fields where links might be stored.
    
    Args:
        snapshot: Facebook ad snapshot dictionary.
    
    Returns:
        List of unique URLs found in the snapshot.
    """
    urls = []
    
    # Common fields where links might be stored
    link_fields = [
        'link_url',
        'cta_url', 
        'website_url',
        'destination_url',
        'landing_page_url',
        'click_url'
    ]
    
    # Check direct link fields
    for field in link_fields:
        url = snapshot.get(field)
        if url and isinstance(url, str) and url.strip():
            urls.append(url.strip())
    
    # Check call_to_action object
    cta = snapshot.get('call_to_action', {})
    if isinstance(cta, dict):
        for field in link_fields:
            url = cta.get(field)
            if url and isinstance(url, str) and url.strip():
                urls.append(url.strip())
        # Also check for nested link
        link_obj = cta.get('link', {})
        if isinstance(link_obj, dict):
            for field in link_fields:
                url = link_obj.get(field)
                if url and isinstance(url, str) and url.strip():
                    urls.append(url.strip())
    
    # Check for outbound_links array
    outbound_links = snapshot.get('outbound_links', [])
    if isinstance(outbound_links, list):
        for link in outbound_links:
            if isinstance(link, str) and link.strip():
                urls.append(link.strip())
            elif isinstance(link, dict):
                for field in link_fields:
                    url = link.get(field)
                    if url and isinstance(url, str) and url.strip():
                        urls.append(url.strip())
    
    # Check for cards (DCO, CAROUSEL, DPA)
    cards = snapshot.get('cards', [])
    if isinstance(cards, list):
        for card in cards:
            if not isinstance(card, dict):
                continue
            # Direct link fields in card
            for field in link_fields:
                card_url = card.get(field)
                if card_url and isinstance(card_url, str) and card_url.strip():
                    urls.append(card_url.strip())
            
            # Nested call_to_action in card
            card_cta = card.get('call_to_action', {})
            if isinstance(card_cta, dict):
                for field in link_fields:
                    cta_url = card_cta.get(field)
                    if cta_url and isinstance(cta_url, str) and cta_url.strip():
                        urls.append(cta_url.strip())
                
                # Nested link object in card CTA
                card_cta_link = card_cta.get('link', {})
                if isinstance(card_cta_link, dict):
                    for field in link_fields:
                        l_url = card_cta_link.get(field)
                        if l_url and isinstance(l_url, str) and l_url.strip():
                            urls.append(l_url.strip())
    
    # Check body text for URLs (regex-like simple extraction)
    body = snapshot.get('body', {})
    if isinstance(body, dict):
        body_text = body.get('text', '')
        if isinstance(body_text, str):
            # Simple URL extraction (basic pattern)
            url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s<>"{}|\\^`\[\].,;:!?]'
            found_urls = re.findall(url_pattern, body_text)
            urls.extend(found_urls)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    return unique_urls


def parse_fb_ads(resJson: Dict[str, Any], trim: bool = True, filter_inactive: bool = True) -> List[Dict[str, Any]]:
    """
    Parse Facebook ads from API response.
    
    Args:
        resJson: The JSON response from the ScrapeCreators API.
        trim: Whether to include only essential fields.
    
    Returns:
        List of parsed ad objects.
    """
    ads = []
    results = resJson.get('results', [])
    logger.info(f"Parsing {len(results)} FB ads")
    
    for ad in results:
        try:
            ad_id = ad.get('ad_archive_id')
            if not ad_id:
                continue

            # Parse dates
            start_date = ad.get('start_date')
            end_date = ad.get('end_date')

            if start_date is not None:
                start_date = datetime.fromtimestamp(start_date).isoformat()
            if end_date is not None:
                end_date = datetime.fromtimestamp(end_date).isoformat()
            
            # Filter out inactive ads: skip if end_date is in the past
            # Only filter if filter_inactive is True (for search API, we already requested ACTIVE)
            if filter_inactive and end_date is not None:
                try:
                    end_date_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00') if 'Z' in end_date else end_date)
                    current_date = datetime.now(end_date_dt.tzinfo) if end_date_dt.tzinfo else datetime.now()
                    if end_date_dt < current_date:
                        logger.debug(f"Skipping inactive ad {ad.get('ad_archive_id')} with end_date {end_date}")
                        continue
                except Exception as e:
                    logger.warning(f"Could not parse end_date {end_date} for ad {ad.get('ad_archive_id')}: {str(e)}")
                    # Continue processing if date parsing fails

            # Parse snapshot data
            snapshot = ad.get('snapshot', {})
            media_type = snapshot.get('display_format')
            
            # Skip unsupported media types
            if media_type not in {'IMAGE', 'VIDEO', 'DCO', 'CAROUSEL', 'DPA', 'MULTI_IMAGES'}:
                continue

            # Parse bodies and titles
            global_body = snapshot.get('body', {}).get('text') if isinstance(snapshot.get('body'), dict) else snapshot.get('body')
            global_title = snapshot.get('title', {}).get('text') if isinstance(snapshot.get('title'), dict) else snapshot.get('title')
            
            bodies = []
            titles = []

            # Parse media URLs based on type
            media_urls = []
            card_media_types = []

            if media_type in ('IMAGE', 'MULTI_IMAGES'):
                images = snapshot.get('images', [])
                for img in images:
                    img_url = (img.get('resized_image_url') or img.get('original_image_url'))
                    if img_url:
                        media_urls.append(img_url)
                        card_media_types.append('IMAGE')
                        bodies.append(global_body)
                        titles.append(global_title)
                
                # For plain IMAGE, limit to first
                if media_type == 'IMAGE' and len(media_urls) > 1:
                    media_urls = media_urls[:1]
                    card_media_types = card_media_types[:1]
                    bodies = bodies[:1]
                    titles = titles[:1]

            elif media_type == 'VIDEO':
                videos = snapshot.get('videos', [])
                if len(videos) > 0:
                    vid_url = (videos[0].get('video_sd_url') or
                              videos[0].get('video_hd_url') or
                              videos[0].get('watermarked_video_sd_url') or
                              videos[0].get('video_preview_image_url'))
                    if vid_url:
                        media_urls = [vid_url]
                        card_media_types = ['VIDEO']
                        bodies = [global_body]
                        titles = [global_title]

            elif media_type in ('DCO', 'CAROUSEL', 'DPA'):
                cards = snapshot.get('cards', [])
                for card in cards:
                    # Detect card media type
                    vid_url = (card.get('video_sd_url') or card.get('video_hd_url'))
                    img_url = (card.get('resized_image_url') or
                              card.get('original_image_url') or
                              card.get('video_preview_image_url'))
                    
                    m_url = vid_url or img_url
                    if not m_url:
                        continue
                        
                    media_urls.append(m_url)
                    card_media_types.append('VIDEO' if vid_url else 'IMAGE')
                    
                    # Card specific text with fallback to global
                    c_body = card.get('body')
                    if isinstance(c_body, dict): c_body = c_body.get('text')
                    bodies.append(c_body if c_body else global_body)
                    
                    c_title = card.get('title')
                    if isinstance(c_title, dict): c_title = c_title.get('text')
                    titles.append(c_title if c_title else global_title)
            
            # Skip only if no media content (body/title can be empty)
            if len(media_urls) == 0:
                continue
            
            # Ensure we have matching counts for media_urls, bodies, and titles
            # Use media_urls count as base (since media is required)
            media_count = len(media_urls)
            
            # Extend bodies and titles to match media_count
            if len(bodies) < media_count:
                first_body = bodies[0] if bodies else None
                bodies.extend([first_body] * (media_count - len(bodies)))
            elif len(bodies) > media_count:
                bodies = bodies[:media_count]
            
            if len(titles) < media_count:
                first_title = titles[0] if titles else None
                titles.extend([first_title] * (media_count - len(titles)))
            elif len(titles) > media_count:
                titles = titles[:media_count]

            # Extract all destination URLs from snapshot
            destination_urls_raw = extract_all_urls_from_snapshot(snapshot)
            
            # Parse each URL with UTM parameters
            destination_urls_parsed = []
            external_urls = []
            internal_urls = []
            all_utm_params = {}
            
            for url in destination_urls_raw:
                parsed_url = parse_url_utm_params(url)
                if parsed_url:
                    destination_urls_parsed.append(parsed_url)
                    # Collect all UTM params
                    if parsed_url.get('utm_params'):
                        all_utm_params.update(parsed_url['utm_params'])
                    # Categorize by internal/external
                    if parsed_url.get('is_internal'):
                        internal_urls.append(parsed_url)
                    else:
                        external_urls.append(parsed_url)

            # Create ad objects
            for idx, (media_url, body_text, title_text) in enumerate(zip(media_urls, bodies, titles)):
                if media_url is not None:  # Only require media, body/title can be empty
                    # Resolve per-card media type (handles CAROUSEL with video cards)
                    resolved_media_type = card_media_types[idx] if idx < len(card_media_types) else media_type
                    ad_obj = {
                        'ad_id': ad_id,
                        'start_date': start_date,
                        'end_date': end_date,
                        'media_url': media_url,
                        'body': body_text or '',  # Empty string if None
                        'title': title_text or '',
                        'media_type': resolved_media_type,
                        'display_format': media_type,  # original format (CAROUSEL, DCO etc.)
                        # URL information - always included
                        'page_id': ad.get('page_id'),
                        'page_name': ad.get('page_name') or snapshot.get('page_name'),
                        'destination_urls': destination_urls_parsed,
                        'destination_urls_full': [u['full_url'] for u in destination_urls_parsed],
                        'external_urls': external_urls,
                        'internal_urls': internal_urls,
                        'has_external_links': len(external_urls) > 0,
                        'utm_params': all_utm_params,
                        'domains': list(set([u['domain'] for u in destination_urls_parsed if u.get('domain')]))
                    }
                    
                    # Add additional fields if not trimming
                    if not trim:
                        ad_obj.update({
                            'currency': ad.get('currency'),
                            'funding_entity': ad.get('funding_entity'),
                            'impressions': ad.get('impressions'),
                            'spend': ad.get('spend'),
                            'disclaimer': ad.get('disclaimer'),
                            'languages': ad.get('languages'),
                            'publisher_platforms': ad.get('publisher_platforms'),
                            'platform_positions': ad.get('platform_positions'),
                            'effective_status': ad.get('effective_status')
                        })
                    
                    ads.append(ad_obj)
                    
        except Exception as e:
            logger.error(f"Error parsing ad {ad.get('ad_archive_id', 'unknown')}: {str(e)}")
            continue

    return ads
