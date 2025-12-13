
import sys
import os
import json
import builtins
import traceback

# 1. SETUP LOGGING & I/O
# Redirect stderr to a log file for debugging
# Redirect stderr to a log file for debugging
current_script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(current_script_dir, "mcp_debug_manual.log")
log_file = open(log_path, "w", encoding="utf-8", buffering=1)
sys.stderr = log_file

def log(msg):
    try:
        sys.stderr.write(str(msg) + "\n")
        sys.stderr.flush()
    except:
        pass

log("Starting manual MCP server...")

# Force binary mode for Windows standard streams
if sys.platform == 'win32':
    import msvcrt
    try:
        msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
        log("Binary mode enabled.")
    except Exception as e:
        log(f"Failed to set binary mode: {e}")

# Monkey-patch print to write to stderr
original_print = builtins.print
def print_to_stderr(*args, **kwargs):
    kwargs["file"] = sys.stderr
    original_print(*args, **kwargs)
builtins.print = print_to_stderr

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
log(f"Current dir in path: {current_dir}")

# 2. IMPORT LOGIC
try:
    log("Importing mcp_library...")
    import mcp_library
    log("Import successful.")
except Exception as e:
    log(f"Failed to import mcp_library: {e}")
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# 3. HELPER TO EXTRACT TOOLS
def get_tools_list():
    tools_info = [
        {
            "name": "get_meta_platform_id",
            "description": "Search for companies/brands in the Meta Ad Library and return their platform IDs.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "brand_names": {
                        "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}],
                        "description": "Brand name(s)"
                    }
                },
                "required": ["brand_names"]
            }
        },
        {
            "name": "search_ads_final",
            "description": "Unified tool to search for ads in the Meta Ad Library. Supports filtering, media analysis (Gemini), and saving to file. Use this for ALL searches.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                    "country": {"type": "string"},
                    "active_status": {"type": "string"},
                    "media_type": {"type": "string"},
                    "analyze_media": {"type": "boolean"},
                    "target_file": {"type": "string"},
                    "append_mode": {"type": "boolean"},
                    "max_ads": {"type": "integer"},
                    "apply_filtering": {"type": "boolean"}
                },
                "required": ["query"]
            }
        },
        {
            "name": "get_meta_ads_external_only",
            "description": "Retrieve ads for brand(s) that lead to external websites (not Meta/Google properties).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "platform_ids": {
                        "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
                    },
                    "limit": {"type": "integer"},
                    "country": {"type": "string"},
                    "min_results": {"type": "integer"}
                },
                "required": ["platform_ids"]
            }
        },
        {
            "name": "analyze_ad_image",
            "description": "Download and analyze ad images to extract visual elements, text content, and composition details using Claude/Gemini.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "media_urls": {
                        "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
                    },
                    "brand_name": {"type": "string"},
                    "ad_id": {"type": "string"}
                },
                "required": ["media_urls"]
            }
        },
        {
            "name": "analyze_ad_video",
            "description": "Download and analyze ad videos utilizing video understanding capabilities.",
            "inputSchema": {
                 "type": "object",
                "properties": {
                    "media_url": {"type": "string"},
                    "brand_name": {"type": "string"},
                    "ad_id": {"type": "string"}
                },
                "required": ["media_url"]
            }
        },
        {
             "name": "get_cache_stats",
             "description": "Get comprehensive statistics about the media cache (images and videos).",
             "inputSchema": {
                 "type": "object",
                 "properties": {},
             }
        },
        {
             "name": "search_cached_media",
             "description": "Find previously analyzed ad media (images and videos) in cache.",
             "inputSchema": {
                 "type": "object",
                 "properties": {
                    "brand_name": {"type": "string"},
                    "has_people": {"type": "boolean"},
                    "color_contains": {"type": "string"},
                    "media_type": {"type": "string"},
                    "limit": {"type": "integer"}
                 }
             }
        },
        {
            "name": "cleanup_media_cache",
            "description": "Clean up old cached media files and free disk space.",
             "inputSchema": {
                 "type": "object",
                 "properties": {
                    "max_age_days": {"type": "integer"}
                 }
             }
        }
    ]
    return tools_info

def call_tool(name, arguments):
    # Dispatch manual calls
    if name == "get_meta_platform_id":
        return mcp_library.get_meta_platform_id(**arguments)
    elif name == "search_ads_final":
        # Map new tool name to the library function
        return mcp_library.search_facebook_ads(**arguments)
    elif name == "get_meta_ads":
        return mcp_library.get_meta_ads(**arguments)

    elif name == "get_meta_ads_external_only":
        return mcp_library.get_meta_ads_external_only(**arguments)
    elif name == "analyze_ad_image":
        return mcp_library.analyze_ad_image(**arguments)
    elif name == "analyze_ad_video":
        return mcp_library.analyze_ad_video(**arguments)
    elif name == "get_cache_stats":
        return mcp_library.get_cache_stats(**arguments)
    elif name == "search_cached_media":
        return mcp_library.search_cached_media(**arguments)
    elif name == "cleanup_media_cache":
        return mcp_library.cleanup_media_cache(**arguments)
    else:
        raise ValueError(f"Unknown tool: {name}")

# 4. MAIN LOOP
log("Entering main loop...")
while True:
    try:
        # Read line (binary safe)
        line_bytes = sys.stdin.buffer.readline()
        if not line_bytes:
            log("EOF received from stdin. Exiting.")
            break
            
        try:
            line = line_bytes.decode('utf-8').strip()
        except UnicodeDecodeError:
            log("Decoding error on input line.")
            continue
            
        if not line:
            continue
            
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            log(f"Invalid JSON received: {line}")
            continue
            
        method = request.get("method")
        msg_id = request.get("id")
        
        log(f"Received request: {method}")
        
        response = None

        if method == "initialize":
            response = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "FacebookAdsMCP_Manual",
                        "version": "1.0"
                    }
                }
            }
        
        elif method == "notifications/initialized":
            log("Client initialized.")
            # No response needed
            continue
            
        elif method == "tools/list":
            response = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "tools": get_tools_list()
                }
            }

        elif method == "tools/call":
            params = request.get("params", {})
            name = params.get("name")
            args = params.get("arguments", {})
            
            try:
                log(f"Calling tool: {name} with args: {args}")
                result_data = call_tool(name, args)
                
                # Format result for MCP (wrap in content list)
                content = []
                content.append({"type": "text", "text": json.dumps(result_data, default=str, ensure_ascii=False)})

                response = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "content": content,
                        "isError": False
                    }
                }
            except Exception as e:
                log(f"Tool error: {e}")
                traceback.print_exc(file=sys.stderr)
                response = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "error": {
                        "code": -32000,
                        "message": str(e)
                    }
                }

        elif method == "ping":
            response = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {}
            }
            
        else:
            # Ignore other methods or unsupported notifications
            log(f"Ignored method: {method}")
            continue

        # Send Response
        if response:
            try:
                response_str = json.dumps(response)
                response_bytes = response_str.encode('utf-8') + b"\n"
                sys.stdout.buffer.write(response_bytes)
                sys.stdout.buffer.flush()
                log(f"Sent response for {method}")
            except Exception as e:
                log(f"Failed to send response: {e}")

    except Exception as e:
        log(f"Loop error: {e}")
        break
