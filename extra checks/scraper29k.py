import re
import os
import json
import hashlib
from hashlib import md5
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
from threading import Lock
from collections import deque, defaultdict, Counter
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

VALID_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu"
)

# Directories
DATA_DIR = "crawl_data"
LOG_DIR = "crawler_logs"

for directory in [DATA_DIR, LOG_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Limits
MAX_CONTENT_SIZE = 5 * 1024 * 1024
MIN_WORD_COUNT = 50
MAX_PATH_DEPTH = 10
MAX_URL_LENGTH = 500

# Duplicate detection settings
SIMHASH_THRESHOLD = 10  # Maximum Hamming distance for near-duplicates
SIMHASH_WINDOW = 1000   # Keep last 1000 simhashes in memory

# File extensions to block
INVALID_EXTENSIONS = {
    'pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx',
    'zip', 'rar', 'tar', 'gz', '7z', 'bz2',
    'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'ico', 'webp',
    'mp4', 'avi', 'mov', 'mp3', 'wav', 'flv',
    'css', 'js', 'json', 'xml', 'rss', 'atom'
}

# ============================================================
# GLOBAL TRACKING
# ============================================================

# Progress tracking
pages_processed = 0
pages_saved = 0
links_discovered = 0
duplicates_found = 0  # New: track duplicates
progress_lock = Lock()

# Duplicate detection
seen_exact_hashes = set()
seen_simhashes = deque(maxlen=SIMHASH_WINDOW)
duplicate_lock = Lock()

# Rejection tracking
rejection_stats = Counter()
rejection_samples = defaultdict(list)
rejection_lock = Lock()

# Processing tracking
processing_log = []
processing_lock = Lock()

# Trap detection
url_pattern_counter = defaultdict(int)
domain_path_counter = defaultdict(lambda: defaultdict(int))
trap_lock = Lock()

# ============================================================
# DUPLICATE DETECTION - SIMHASH
# ============================================================

def compute_simhash(text, hash_bits=64):
    """
    Compute simhash fingerprint for near-duplicate detection
    
    Algorithm:
    1. Split text into 3-word shingles (n-grams)
    2. Hash each shingle
    3. Build frequency vector
    4. Generate fingerprint
    
    Time Complexity: O(n) where n is number of words
    """
    words = text.split()
    
    # Handle very short text
    if len(words) < 3:
        return hash(text) & ((1 << hash_bits) - 1)
    
    # For large documents, sample shingles for performance
    if len(words) > 500:
        step = len(words) // 250
        shingles = [' '.join(words[i:i+3]) for i in range(0, len(words) - 2, step)]
    else:
        shingles = [' '.join(words[i:i+3]) for i in range(len(words) - 2)]
    
    if not shingles:
        return 0
    
    # Initialize vector for weighted bits
    vector = [0] * hash_bits
    
    # Hash each shingle and update vector
    for shingle in shingles:
        shingle_hash = hash(shingle) & ((1 << hash_bits) - 1)
        
        # Update vector based on bits in hash
        for i in range(hash_bits):
            if shingle_hash & (1 << i):
                vector[i] += 1
            else:
                vector[i] -= 1
    
    # Generate final fingerprint
    fingerprint = 0
    for i in range(hash_bits):
        if vector[i] > 0:
            fingerprint |= (1 << i)
    
    return fingerprint

def hamming_distance(hash1, hash2):
    """
    Calculate Hamming distance between two hashes
    (Number of differing bits)
    
    Time Complexity: O(1)
    """
    return bin(hash1 ^ hash2).count('1')

def is_duplicate(text_content, url):
    """
    Check if content is duplicate or near-duplicate
    
    Returns: (is_dup, reason)
    - is_dup: True if duplicate
    - reason: 'exact' or 'similar' or None
    """
    global duplicates_found
    
    # Minimum content check
    if not text_content or len(text_content.strip()) < 100:
        return True, 'too_short'
    
    # Compute exact hash
    content_hash = hashlib.md5(text_content.encode('utf-8', errors='ignore')).hexdigest()
    
    with duplicate_lock:
        # Check exact duplicate
        if content_hash in seen_exact_hashes:
            duplicates_found += 1
            return True, 'exact'
        
        # Add to exact hash set
        seen_exact_hashes.add(content_hash)
    
    # Compute simhash for near-duplicate detection
    content_simhash = compute_simhash(text_content)
    
    with duplicate_lock:
        # Check near-duplicates (last N pages only for performance)
        for seen_hash, seen_url in seen_simhashes:
            distance = hamming_distance(content_simhash, seen_hash)
            
            if distance <= SIMHASH_THRESHOLD:
                duplicates_found += 1
                return True, f'similar_to_{seen_url[:50]}'
        
        # Not a duplicate - add to recent simhashes
        seen_simhashes.append((content_simhash, url))
    
    return False, None

# ============================================================
# LOGGING SYSTEM
# ============================================================

def log_processing(url, status, reason="", links_found=0):
    """
    Log every URL processed with reason
    Status: 'processed', 'rejected', 'skipped', 'error', 'duplicate'
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with processing_lock:
        processing_log.append({
            'timestamp': timestamp,
            'url': url,
            'status': status,
            'reason': reason,
            'links_found': links_found
        })
    
    # Save log periodically
    if len(processing_log) % 100 == 0:
        save_processing_log()

def save_processing_log():
    """Save processing log to file"""
    try:
        log_file = os.path.join(LOG_DIR, 'processing_log.jsonl')
        
        with processing_lock:
            with open(log_file, 'a') as f:
                for entry in processing_log:
                    f.write(json.dumps(entry) + '\n')
            processing_log.clear()
    except Exception as e:
        print(f"[ERROR] Could not save processing log: {e}")

def log_rejection(reason, url, save_sample=True):
    """Log URL rejection with reason"""
    with rejection_lock:
        rejection_stats[reason] += 1
        
        if save_sample and len(rejection_samples[reason]) < 5:
            rejection_samples[reason].append(url)

def print_progress():
    """Print current progress"""
    with progress_lock:
        print(f"\n{'='*80}")
        print(f"CRAWLER PROGRESS")
        print(f"{'='*80}")
        print(f"Pages processed:    {pages_processed:,}")
        print(f"Pages saved:        {pages_saved:,}")
        print(f"Duplicates found:   {duplicates_found:,}")
        print(f"Links discovered:   {links_discovered:,}")
        print(f"{'='*80}\n")

# ============================================================
# CALENDAR & TRAP DETECTION
# ============================================================

CALENDAR_PATTERNS = [
    r'/calendar',
    r'/events?/',
    r'/event-calendar',
    r'/ical',
    r'/\.ics$',
    r'[?&]calendar',
    r'[?&]event',
    r'[?&]date=',
    r'[?&]month=',
    r'[?&]year=',
    r'/\d{4}/\d{2}/\d{2}',
]

TRAP_PATTERNS = [
    r'/wp-admin',
    r'/wp-login',
    r'/login',
    r'/logout',
    r'/signin',
    r'/signout',
    r'/register',
    r'/signup',
    r'/user/',
    r'/account',
    r'/profile',
    r'/dashboard',
    r'/admin',
]

LEGITIMATE_PATTERNS = [
    r'/wiki/',
    r'/archive/',
    r'/docs/',
    r'/pub/',
    r'/repository/',
    r'/faculty/',
    r'/courses?/',
    r'/research/',
    r'/projects?/',
]



def is_calendar_or_event(url):
    """Detect calendar and event pages"""
    url_lower = url.lower()
    
    for pattern in CALENDAR_PATTERNS:
        if re.search(pattern, url_lower):
            return True
    
    return False

def is_known_trap(url):
    """Detect known crawler traps"""
    url_lower = url.lower()
    
    for pattern in TRAP_PATTERNS:
        if re.search(pattern, url_lower):
            return True
    
    return False

def is_url_trap(url):
    """Advanced trap detection"""
    try:
        parsed = urlparse(url)
        
        # Path depth check
        path_parts = [p for p in parsed.path.split('/') if p]
        max_depth = 15 if is_legitimate_pattern(url) else MAX_PATH_DEPTH
        
        if len(path_parts) > max_depth:
            return True  

        # Repeating path segments
        if not is_legitimate_pattern(url):
            segment_counts = Counter(path_parts)
            if any(count > 3 for count in segment_counts.values()):
                return True   

        # Pattern frequency tracking
        pattern = get_url_pattern(url)
        
        with trap_lock:
            url_pattern_counter[pattern] += 1
            max_repeats = 150 if is_legitimate_pattern(url) else 50
            
            if url_pattern_counter[pattern] > max_repeats:
                return True  

        # Pagination limits
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for param in ['page', 'p', 'offset', 'start']:
                if param in query_params:
                    try:
                        page_num = int(query_params[param][0])
                        if page_num > 200:
                            return True
                    except (ValueError, IndexError):
                        pass
        
        # Query length
        if len(parsed.query) > 150:
            return True
        
        # Filter/sort combinations
        if parsed.query:
            query_lower = parsed.query.lower()
            filter_params = ['sort', 'order', 'filter', 'view', 'display']
            count = sum(1 for p in filter_params if p in query_lower)
            if count >= 3:
                return True
                domain = parsed.netloc
        
        path = parsed.path
        
        with trap_lock:
            domain_path_counter[domain][path] += 1
            
            # If same exact path accessed more than 10 times, likely a trap
            # (unless it's a legitimate pattern)
            max_same_path = 20 if is_legitimate_pattern(url) else 10
            
            if domain_path_counter[domain][path] > max_same_path:
                return True

        return False
    
    except Exception:
        return False

def get_url_pattern(url):
    """Get URL pattern for trap detection"""
    try:
        parsed = urlparse(url)
        path = re.sub(r'\d+', 'N', parsed.path)
        path = re.sub(r'\d{4}-\d{2}-\d{2}', 'DATE', path)
        query_keys = '&'.join(sorted(parse_qs(parsed.query).keys())) if parsed.query else ''
        return f"{parsed.netloc}{path}?{query_keys}"
    except Exception:
        return url

# ============================================================
# URL VALIDATION
# ============================================================

def is_valid(url):
    """
    Comprehensive URL validation with logging
    Returns True if URL should be crawled
    """
    try:
        parsed = urlparse(url)
        
        # Scheme check
        if parsed.scheme not in {"http", "https"}:
            log_rejection("bad_scheme", url)
            return False
        
        # Domain check
        netloc = parsed.netloc.lower()
        if not any(netloc.endswith(domain) for domain in VALID_DOMAINS):
            log_rejection("bad_domain", url)
            return False
        
        # URL length
        if len(url) > MAX_URL_LENGTH:
            log_rejection("url_too_long", url)
            return False
        
        # Calendar/Event pages
        if is_calendar_or_event(url):
            log_rejection("calendar_event", url)
            return False
        
        # Known traps
        if is_known_trap(url):
            log_rejection("known_trap", url)
            return False
        
        # Dynamic traps
        if is_url_trap(url):
            log_rejection("url_trap", url)
            return False
        
        path_lower = parsed.path.lower()
        
        # File extensions
        if "." in path_lower:
            parts = path_lower.rsplit('.', 1)
            if len(parts) == 2:
                ext = parts[1].split('/')[0].split('?')[0]
                if ext in INVALID_EXTENSIONS:
                    log_rejection(f"extension_{ext}", url, save_sample=False)
                    return False
        
        # Search/print/share endpoints
        if any(x in url.lower() for x in ['/search?', '?search=', '/print/', '?print=', '/share/', '?share=']):
            log_rejection("action_endpoint", url, save_sample=False)
            return False
        
        return True
    
    except Exception as e:
        log_rejection(f"exception_{str(e)[:20]}", url, save_sample=False)
        return False

# ============================================================
# MAIN SCRAPER
# ============================================================

def scraper(url, resp):
    """
    Main scraper function with comprehensive logging and duplicate detection
    """
    global pages_processed, pages_saved, links_discovered
    
    # Update progress
    with progress_lock:
        pages_processed += 1
        
        # Print progress every 10 pages
        if pages_processed % 10 == 0:
            print(f"[PROGRESS] Processed: {pages_processed}, Saved: {pages_saved}, Duplicates: {duplicates_found}, Links: {links_discovered}")
    
    # Check response
    if not resp or resp.status != 200:
        log_processing(url, 'error', f'status_{resp.status if resp else "none"}')
        return []
    
    try:
        # Get content
        content = resp.raw_response.content
        
        if not content or len(content) < 100:
            log_processing(url, 'skipped', 'empty_content')
            return []
        
        if len(content) > MAX_CONTENT_SIZE:
            log_processing(url, 'skipped', 'too_large')
            return []
        
        # Parse HTML
        soup = BeautifulSoup(content, "lxml")
        
        # Extract text
        text_content = extract_text_content_from_soup(soup)
        
        # Check word count
        words = text_content.split()
        if len(words) < MIN_WORD_COUNT:
            log_processing(url, 'skipped', f'low_words_{len(words)}')
            return []
        
        # ===== DUPLICATE DETECTION =====
        is_dup, dup_reason = is_duplicate(text_content, url)
        
        if is_dup:
            log_processing(url, 'duplicate', dup_reason)
            # Still extract links even from duplicates
            links = extract_next_links_from_soup(url, soup)
            valid_links = [link for link in links if is_valid(link)]
            
            with progress_lock:
                links_discovered += len(valid_links)
            
            return valid_links
        
        # ===== NOT A DUPLICATE - PROCEED =====
        
        # Extract links
        links = extract_next_links_from_soup(url, soup)
        
        # Filter valid links
        valid_links = [link for link in links if is_valid(link)]
        
        # Update link count
        with progress_lock:
            links_discovered += len(valid_links)
        
        # Save page data (only unique, quality content)
        if len(valid_links) > 0 or len(words) > 200:
            save_page_data(url, words, text_content)
            with progress_lock:
                pages_saved += 1
        
        # Log successful processing
        log_processing(url, 'processed', 'success', len(valid_links))
        
        return valid_links
    
    except Exception as e:
        log_processing(url, 'error', f'exception_{str(e)[:30]}')
        return []

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def extract_text_content_from_soup(soup):
    """Extract visible text from HTML"""
    try:
        for tag in soup(["script", "style", "meta", "link", "noscript", "header", "footer", "nav"]):
            tag.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    except Exception:
        return ""

def extract_next_links_from_soup(url, soup):
    """Extract all valid links from HTML"""
    links = set()
    
    try:
        for tag in soup.find_all("a", href=True):
            href = tag.get("href")
            
            if not href or href.strip() in ['#', 'javascript:void(0)', 'javascript:;']:
                continue
            
            try:
                absolute_url = urljoin(url, href)
            except Exception:
                continue
            
            clean_url, _ = urldefrag(absolute_url)
            
            if clean_url and clean_url != url:
                links.add(clean_url)
    except Exception:
        pass
    
    return list(links)

def save_page_data(url, words, text_content):
    """Save page metadata to JSON"""
    try:
        url_hash = md5(url.encode('utf-8')).hexdigest()
        
        # Compute content hash for verification
        content_hash = hashlib.md5(text_content.encode('utf-8', errors='ignore')).hexdigest()
        
        data = {
            'url': url,
            'word_count': len(words),
            'words': words[:1000],  # First 1000 words
            'content_hash': content_hash  # Store hash for verification
        }
        
        with open(os.path.join(DATA_DIR, f"{url_hash}.json"), 'w') as f:
            json.dump(data, f)
    except Exception:
        pass

# ============================================================
# REPORTING
# ============================================================

def print_final_report():
    """Print final statistics on exit"""
    print("\n" + "=" * 80)
    print("CRAWLER FINAL REPORT")
    print("=" * 80)
    
    with progress_lock:
        print(f"\n📊 PAGES:")
        print(f"  Processed:    {pages_processed:,}")
        print(f"  Saved:        {pages_saved:,}")
        print(f"  Duplicates:   {duplicates_found:,}")
        print(f"  Links found:  {links_discovered:,}")
        
        # Calculate efficiency
        if pages_processed > 0:
            dup_rate = (duplicates_found / pages_processed) * 100
            save_rate = (pages_saved / pages_processed) * 100
            print(f"\n  Duplicate rate: {dup_rate:.1f}%")
            print(f"  Save rate:      {save_rate:.1f}%")
    
    print(f"\n🔍 DUPLICATE DETECTION:")
    with duplicate_lock:
        print(f"  Exact hashes tracked:  {len(seen_exact_hashes):,}")
        print(f"  Simhashes tracked:     {len(seen_simhashes):,}")
        print(f"  Simhash threshold:     {SIMHASH_THRESHOLD} bits")
    
    print(f"\n❌ REJECTIONS:")
    with rejection_lock:
        total_rejected = sum(rejection_stats.values())
        print(f"  Total:        {total_rejected:,}\n")
        
        # Top 10 rejection reasons
        for reason, count in rejection_stats.most_common(10):
            pct = (count / total_rejected * 100) if total_rejected > 0 else 0
            print(f"  {reason:30s} {count:8,} ({pct:5.1f}%)")
            
            # Show samples
            if reason in rejection_samples and rejection_samples[reason]:
                for sample_url in rejection_samples[reason][:2]:
                    print(f"    Sample: {sample_url}")
    
    print("\n" + "=" * 80)
    
    # Save final logs
    save_processing_log()
    save_rejection_report()
    save_duplicate_report()

def save_rejection_report():
    """Save detailed rejection report"""
    try:
        report_file = os.path.join(LOG_DIR, 'rejection_report.txt')
        
        with open(report_file, 'w') as f:
            f.write("URL REJECTION REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            with rejection_lock:
                total = sum(rejection_stats.values())
                f.write(f"Total rejected: {total:,}\n\n")
                
                for reason, count in rejection_stats.most_common():
                    pct = (count / total * 100) if total > 0 else 0
                    f.write(f"\n{reason}: {count:,} ({pct:.1f}%)\n")
                    
                    if reason in rejection_samples:
                        f.write("  Sample URLs:\n")
                        for url in rejection_samples[reason]:
                            f.write(f"    - {url}\n")
    except Exception:
        pass

def save_duplicate_report():
    """Save duplicate detection report"""
    try:
        report_file = os.path.join(LOG_DIR, 'duplicate_report.txt')
        
        with open(report_file, 'w') as f:
            f.write("DUPLICATE DETECTION REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            with progress_lock, duplicate_lock:
                f.write(f"Total pages processed:  {pages_processed:,}\n")
                f.write(f"Duplicates found:       {duplicates_found:,}\n")
                f.write(f"Unique pages saved:     {pages_saved:,}\n\n")
                
                if pages_processed > 0:
                    dup_rate = (duplicates_found / pages_processed) * 100
                    f.write(f"Duplicate rate:         {dup_rate:.1f}%\n\n")
                
                f.write(f"Algorithm:\n")
                f.write(f"  - Exact duplicate detection: MD5 hash\n")
                f.write(f"  - Near-duplicate detection: Simhash\n")
                f.write(f"  - Simhash threshold: {SIMHASH_THRESHOLD} bits\n")
                f.write(f"  - Simhash window: {SIMHASH_WINDOW} recent pages\n\n")
                
                f.write(f"Statistics:\n")
                f.write(f"  - Exact hashes tracked: {len(seen_exact_hashes):,}\n")
                f.write(f"  - Simhashes in memory: {len(seen_simhashes):,}\n")
    except Exception:
        pass

# ============================================================
# CLEANUP
# ============================================================

import atexit

atexit.register(print_final_report)

# ============================================================
# PROGRESS GETTER (for monitoring)
# ============================================================

def get_progress_stats():
    """Get current progress statistics"""
    with progress_lock:
        return {
            'processed': pages_processed,
            'saved': pages_saved,
            'duplicates': duplicates_found,
            'discovered': links_discovered
        }