import re
import hashlib
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from threading import Lock

VALID_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu"
)

# Thread-safe storage for duplicate detection
seen_exact_hashes = set()
seen_simhashes = []  # List of (simhash, url) tuples
content_lock = Lock()  # Lock for accessing duplicate detection data
SIMHASH_THRESHOLD = 10  # Hamming distance threshold for near-duplicates

def scraper(url, resp):
    """Main scraper function called by workers"""
    # Check for duplicate content first
    if is_duplicate_content(url, resp):
        return []
    
    # Extract and filter links
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    """Extract all links from the HTML page"""
    links = set()
    
    try:
        # Check if response is valid
        if resp is None or resp.status != 200:
            return []
        
        content = resp.raw_response.content
        if not content:
            return []
        
        # Parse HTML
        soup = BeautifulSoup(content, "lxml")
        
        # Find all <a> tags with href attribute
        for tag in soup.find_all("a", href=True):
            href = tag.get("href")
            
            # Convert to absolute URL
            absolute_url = urljoin(url, href)
            
            # Remove URL fragments (#section)
            clean_url, _ = urldefrag(absolute_url)
            
            # Add to set (automatically handles duplicates)
            if clean_url:
                links.add(clean_url)
    
    except Exception as e:
        print(f"Error extracting links from {url}: {e}")
        return []
    
    return list(links)

def is_valid(url):
    """Check if URL should be crawled"""
    try:
        parsed = urlparse(url)
        
        # Only http/https
        if parsed.scheme not in {"http", "https"}:
            return False
        
        # Only valid UCI domains
        netloc = parsed.netloc.lower()
        if not any(netloc.endswith(domain) for domain in VALID_DOMAINS):
            return False
        
        # Filter out files by extension
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
            r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower()
        ):
            return False
        
        # Filter out calendar pages
        if "calendar" in parsed.path.lower():
            return False
        
        # Filter out very long query strings
        if len(parsed.query) > 100:
            return False
        
        return True
    
    except TypeError:
        print(f"TypeError for {url}")
        return False


# ============================================================
# DUPLICATE DETECTION (Thread-Safe)
# ============================================================

def is_duplicate_content(url, resp):
    """
    Thread-safe duplicate detection using exact hash and simhash.
    Returns True if duplicate (should skip), False if unique.
    """
    try:
        content = resp.raw_response.content
        if not content:
            return True  # Empty content is duplicate
        
        # Extract and normalize text content
        text_content = extract_text_content(content)
        
        if not text_content or len(text_content.strip()) < 50:
            return True  # Too little content
        
        # Thread-safe duplicate checking
        with content_lock:
            # Check exact duplicate
            content_hash = compute_hash(text_content)
            if content_hash in seen_exact_hashes:
                return True
            
            # Check near-duplicate using simhash
            content_simhash = compute_simhash(text_content)
            if is_near_duplicate(content_simhash):
                return True
            
            # Not a duplicate - store fingerprints
            seen_exact_hashes.add(content_hash)
            seen_simhashes.append((content_simhash, url))
        
        return False
    
    except Exception:
        return False  # If error, allow crawling


def extract_text_content(html_content):
    """Extract visible text from HTML"""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        
        # Remove non-content tags
        for tag in soup(["script", "style", "meta", "link", "noscript", "header", "footer", "nav"]):
            tag.decompose()
        
        # Get text and normalize
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text.lower())
        
        return text.strip()
    
    except Exception:
        return ""


def compute_hash(text):
    """Compute MD5 hash for exact duplicate detection"""
    return hashlib.md5(text.encode('utf-8', errors='ignore')).hexdigest()


def compute_simhash(text, hash_bits=64):
    """Compute simhash fingerprint for near-duplicate detection"""
    tokens = create_shingles(text, n=3)
    
    if not tokens:
        return 0
    
    # Initialize bit vector
    vector = [0] * hash_bits
    
    # Process each token
    for token in tokens:
        token_hash = hash(token) & ((1 << hash_bits) - 1)
        
        for i in range(hash_bits):
            if token_hash & (1 << i):
                vector[i] += 1
            else:
                vector[i] -= 1
    
    # Create fingerprint
    fingerprint = 0
    for i in range(hash_bits):
        if vector[i] > 0:
            fingerprint |= (1 << i)
    
    return fingerprint


def create_shingles(text, n=3):
    """Create n-grams (shingles) from text"""
    words = text.split()
    if len(words) < n:
        return [text] if text else []
    
    shingles = []
    for i in range(len(words) - n + 1):
        shingle = ' '.join(words[i:i+n])
        shingles.append(shingle)
    
    return shingles


def hamming_distance(hash1, hash2):
    """Calculate number of differing bits"""
    xor = hash1 ^ hash2
    distance = 0
    while xor:
        distance += 1
        xor &= xor - 1
    return distance


def is_near_duplicate(simhash):
    """Check if simhash is similar to any seen simhash"""
    for seen_hash, seen_url in seen_simhashes:
        distance = hamming_distance(simhash, seen_hash)
        if distance <= SIMHASH_THRESHOLD:
            return True
    return False