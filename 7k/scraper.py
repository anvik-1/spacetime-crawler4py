#!/usr/bin/env python3
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

DATA_DIR = "crawl_data"
LOG_DIR = "crawler_logs"

for directory in [DATA_DIR, LOG_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Limits - BALANCED
MAX_CONTENT_SIZE = 5 * 1024 * 1024
MIN_WORD_COUNT = 50
MAX_PATH_DEPTH = 12  # Increased from 10
MAX_URL_LENGTH = 600  # Increased from 500

SIMHASH_THRESHOLD = 10
SIMHASH_WINDOW = 1000

# Extensions
INVALID_EXTENSIONS = {
    'pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx',
    'txt', 'rtf', 'odt', 'ods', 'odp',
    'zip', 'rar', 'tar', 'gz', '7z', 'bz2',
    'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'ico', 'webp',
    'mp4', 'avi', 'mov', 'mpg', 'mpeg', 'flv',
    'mp3', 'wav', 'wma', 'aac',
    'css', 'js', 'json', 'xml', 'rss', 'atom', 'csv',
    'lif', 'mat', 'dat', 'rdata', 'rds',
    'arff', 'weka', 'sav', 'pkl', 'pickle',
    'log', 'bak', 'tmp'
}

# Allow these web page formats
ALLOWED_EXTENSIONS = {'html', 'htm', 'php', 'asp', 'aspx', 'jsp', 'shtml', 'xhtml'}

# ============================================================
# GLOBAL TRACKING
# ============================================================

pages_processed = 0
pages_saved = 0
links_discovered = 0
duplicates_found = 0
progress_lock = Lock()

seen_exact_hashes = set()
seen_simhashes = deque(maxlen=SIMHASH_WINDOW)
duplicate_lock = Lock()

rejection_stats = Counter()
rejection_samples = defaultdict(list)
rejection_lock = Lock()

processing_log = []
processing_lock = Lock()

url_pattern_counter = defaultdict(int)
domain_path_counter = defaultdict(lambda: defaultdict(int))
trap_lock = Lock()

# ============================================================
# DUPLICATE DETECTION
# ============================================================

def compute_simhash(text, hash_bits=64):
    words = text.split()
    if len(words) < 3:
        return hash(text) & ((1 << hash_bits) - 1)
    
    if len(words) > 500:
        step = len(words) // 250
        shingles = [' '.join(words[i:i+3]) for i in range(0, len(words) - 2, step)]
    else:
        shingles = [' '.join(words[i:i+3]) for i in range(len(words) - 2)]
    
    if not shingles:
        return 0
    
    vector = [0] * hash_bits
    for shingle in shingles:
        shingle_hash = hash(shingle) & ((1 << hash_bits) - 1)
        for i in range(hash_bits):
            if shingle_hash & (1 << i):
                vector[i] += 1
            else:
                vector[i] -= 1
    
    fingerprint = 0
    for i in range(hash_bits):
        if vector[i] > 0:
            fingerprint |= (1 << i)
    
    return fingerprint

def hamming_distance(hash1, hash2):
    return bin(hash1 ^ hash2).count('1')

def is_duplicate(text_content, url):
    global duplicates_found
    
    if not text_content or len(text_content.strip()) < 100:
        return True, 'too_short'
    
    content_hash = hashlib.md5(text_content.encode('utf-8', errors='ignore')).hexdigest()
    
    with duplicate_lock:
        if content_hash in seen_exact_hashes:
            duplicates_found += 1
            return True, 'exact'
        seen_exact_hashes.add(content_hash)
    
    content_simhash = compute_simhash(text_content)
    
    with duplicate_lock:
        for seen_hash, seen_url in seen_simhashes:
            if hamming_distance(content_simhash, seen_hash) <= SIMHASH_THRESHOLD:
                duplicates_found += 1
                return True, 'similar'
        seen_simhashes.append((content_simhash, url))
    
    return False, None

# ============================================================
# LOGGING
# ============================================================

def log_processing(url, status, reason="", links_found=0):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with processing_lock:
        processing_log.append({
            'timestamp': timestamp,
            'url': url,
            'status': status,
            'reason': reason,
            'links_found': links_found
        })
    if len(processing_log) % 100 == 0:
        save_processing_log()

def save_processing_log():
    try:
        with processing_lock:
            if not processing_log:
                return
            with open(os.path.join(LOG_DIR, 'processing_log.jsonl'), 'a') as f:
                for entry in processing_log:
                    f.write(json.dumps(entry) + '\n')
            processing_log.clear()
    except Exception:
        pass

def log_rejection(reason, url, save_sample=True):
    with rejection_lock:
        rejection_stats[reason] += 1
        if save_sample and len(rejection_samples[reason]) < 5:
            rejection_samples[reason].append(url)

# ============================================================
# PATTERNS - BALANCED
# ============================================================

CALENDAR_PATTERNS = [
    r'/calendar', r'/events?/', r'/event-calendar', r'/ical',
    r'/\\.ics$', r'[?&]calendar', r'[?&]event',
    r'[?&]date=', r'[?&]month=', r'[?&]year=',
    r'/\\d{4}/\\d{2}/\\d{2}',
]

TRAP_PATTERNS = [
    r'/wp-admin', r'/wp-login', r'/login', r'/logout',
    r'/signin', r'/signout', r'/register', r'/signup',
    r'/user/', r'/account', r'/profile', r'/dashboard', r'/admin',
]

LEGITIMATE_PATTERNS = [
    r'/wiki/', r'/archive/', r'/docs/', r'/pub/',
    r'/repository/', r'/faculty/', r'/courses?/',
    r'/research/', r'/projects?/', r'/publications?/',
]

def is_calendar_or_event(url):
    return any(re.search(p, url.lower()) for p in CALENDAR_PATTERNS)

def is_known_trap(url):
    return any(re.search(p, url.lower()) for p in TRAP_PATTERNS)

def is_legitimate_pattern(url):
    return any(re.search(p, url.lower()) for p in LEGITIMATE_PATTERNS)

def is_url_trap(url):
    try:
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        
        # 1. Path depth - more lenient for legitimate patterns
        max_depth = 15 if is_legitimate_pattern(url) else MAX_PATH_DEPTH
        if len(path_parts) > max_depth:
            return True
        
        # 2. Repeating segments - ONLY if excessive
        if not is_legitimate_pattern(url):
            segment_counts = Counter(path_parts)
            # Block only if a segment repeats MORE than 3 times
            if any(count > 3 for count in segment_counts.values()):
                return True
        
        # 3. Pattern frequency - increased limits
        pattern = get_url_pattern(url)
        with trap_lock:
            url_pattern_counter[pattern] += 1
            max_repeats = 150 if is_legitimate_pattern(url) else 75  # Increased from 50
            if url_pattern_counter[pattern] > max_repeats:
                return True
        
        # 4. Pagination - increased limit
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for param in ['page', 'p', 'offset', 'start']:
                if param in query_params:
                    try:
                        if int(query_params[param][0]) > 200:  # Increased from 100
                            return True
                    except (ValueError, IndexError):
                        pass
        
        # 5. Query length - increased
        if len(parsed.query) > 200:  # Increased from 150
            return True
        
        # 6. Filter combinations - more lenient
        if parsed.query:
            query_lower = parsed.query.lower()
            filter_params = ['sort', 'order', 'filter', 'view', 'display']
            if sum(1 for p in filter_params if p in query_lower) >= 4:  # Increased from 3
                return True
        
        # 7. Same path repeats
        with trap_lock:
            domain_path_counter[parsed.netloc][parsed.path] += 1
            max_same = 25 if is_legitimate_pattern(url) else 15  # Increased
            if domain_path_counter[parsed.netloc][parsed.path] > max_same:
                return True
        
        return False
    except Exception:
        return False

def get_url_pattern(url):
    try:
        parsed = urlparse(url)
        path = re.sub(r'\\d+', 'N', parsed.path)
        path = re.sub(r'\\d{4}-\\d{2}-\\d{2}', 'DATE', path)
        
        if is_legitimate_pattern(url):
            return f"{parsed.netloc}{path}"
        
        query_keys = '&'.join(sorted(parse_qs(parsed.query).keys())) if parsed.query else ''
        return f"{parsed.netloc}{path}?{query_keys}"
    except Exception:
        return url

# ============================================================
# URL VALIDATION - BALANCED
# ============================================================

def is_valid(url):
    """Balanced URL validation"""
    try:
        parsed = urlparse(url)
        
        # 1. Scheme
        if parsed.scheme not in {"http", "https"}:
            log_rejection("bad_scheme", url, False)
            return False
        
        # 2. Domain - STRICT
        netloc = parsed.netloc.lower()
        is_valid_domain = any(
            netloc == d or netloc.endswith('.' + d) 
            for d in VALID_DOMAINS
        )
        
        if not is_valid_domain:
            log_rejection("bad_domain", url, False)
            return False
        
        # 3. Blocked domains
        blocked = [
            'physics.uci.edu', 'economics.uci.edu', 'chem.uci.edu',
            'bio.uci.edu', 'math.uci.edu', 'engineering.uci.edu',
            'cecs.uci.edu', 'eecs.uci.edu', 'nacs.uci.edu',
        ]
        
        if any(netloc == b or netloc.endswith('.' + b) for b in blocked):
            log_rejection("blocked_domain", url, False)
            return False
        
        # 4. URL length
        if len(url) > MAX_URL_LENGTH:
            log_rejection("url_too_long", url, False)
            return False
        
        # 5. Calendar/events
        if is_calendar_or_event(url):
            log_rejection("calendar_event", url)
            return False
        
        # 6. Known traps
        if is_known_trap(url):
            log_rejection("known_trap", url)
            return False
        
        # 7. Dynamic traps
        if is_url_trap(url):
            log_rejection("url_trap", url, False)
            return False
        
        path_lower = parsed.path.lower()
        
        # 8. File extensions - BALANCED
        if "." in path_lower:
            parts = path_lower.rsplit('.', 1)
            if len(parts) == 2:
                ext = parts[1].split('/')[0].split('?')[0]
                
                # Allow: no extension, allowed extensions
                # Block: invalid extensions that are NOT in allowed list
                if ext and ext not in ALLOWED_EXTENSIONS and ext in INVALID_EXTENSIONS:
                    log_rejection(f"ext_{ext}", url, False)
                    return False
        
        # 9. Format parameters
        if parsed.query:
            query_lower = parsed.query.lower()
            bad_formats = ['format=txt', 'format=pdf', 'format=csv',
                         'export=txt', 'export=pdf', 'download=']
            if any(b in query_lower for b in bad_formats):
                log_rejection("format_param", url, False)
                return False
        
        # 10. Action endpoints
        if any(x in url.lower() for x in ['/search?', '?search=', '/print/', '?print=']):
            log_rejection("action", url, False)
            return False
        
        return True
    
    except Exception:
        return False

# ============================================================
# SCRAPER
# ============================================================

def scraper(url, resp):
    global pages_processed, pages_saved, links_discovered
    
    with progress_lock:
        pages_processed += 1
        if pages_processed % 10 == 0:
            print(f"[PROGRESS] Processed: {pages_processed}, Saved: {pages_saved}, Duplicates: {duplicates_found}, Links: {links_discovered}")
    
    if not resp or resp.status != 200:
        log_processing(url, 'error', f'status_{resp.status if resp else "none"}')
        return []
    
    try:
        content = resp.raw_response.content
        
        if not content or len(content) < 100:
            log_processing(url, 'skipped', 'empty')
            return []
        
        if len(content) > MAX_CONTENT_SIZE:
            log_processing(url, 'skipped', 'large')
            return []
        
        soup = BeautifulSoup(content, "lxml")
        text_content = extract_text_content_from_soup(soup)
        words = text_content.split()
        
        if len(words) < MIN_WORD_COUNT:
            log_processing(url, 'skipped', f'low_words_{len(words)}')
            return []
        
        is_dup, dup_reason = is_duplicate(text_content, url)
        
        if is_dup:
            log_processing(url, 'duplicate', dup_reason)
            links = extract_next_links_from_soup(url, soup)
            valid_links = [link for link in links if is_valid(link)]
            with progress_lock:
                links_discovered += len(valid_links)
            return valid_links
        
        links = extract_next_links_from_soup(url, soup)
        valid_links = [link for link in links if is_valid(link)]
        
        with progress_lock:
            links_discovered += len(valid_links)
        
        if len(valid_links) > 0 or len(words) > 200:
            save_page_data(url, words, text_content)
            with progress_lock:
                pages_saved += 1
        
        log_processing(url, 'processed', 'success', len(valid_links))
        return valid_links
    
    except Exception as e:
        log_processing(url, 'error', str(e)[:30])
        return []

def extract_text_content_from_soup(soup):
    try:
        for tag in soup(["script", "style", "meta", "link", "noscript", "header", "footer", "nav"]):
            tag.decompose()
        text = soup.get_text(separator=' ', strip=True)
        return re.sub(r's+', ' ', text).strip()
    except Exception:
        return ""

def extract_next_links_from_soup(url, soup):
    links = set()
    try:
        for tag in soup.find_all("a", href=True):
            href = tag.get("href")
            if not href or href.strip() in ['#', 'javascript:void(0)', 'javascript:;']:
                continue
            try:
                absolute_url = urljoin(url, href)
                clean_url, _ = urldefrag(absolute_url)
                if clean_url and clean_url != url:
                    links.add(clean_url)
            except Exception:
                continue
    except Exception:
        pass
    return list(links)

def save_page_data(url, words, text_content):
    try:
        url_hash = md5(url.encode('utf-8')).hexdigest()
        data = {
            'url': url,
            'word_count': len(words),
            'words': words[:1000],
            'content_hash': hashlib.md5(text_content.encode('utf-8', errors='ignore')).hexdigest()
        }
        with open(os.path.join(DATA_DIR, f"{url_hash}.json"), 'w') as f:
            json.dump(data, f)
    except Exception:
        pass

# ============================================================
# REPORTING
# ============================================================

def print_final_report():
    print("n" + "=" * 80)
    print("CRAWLER FINAL REPORT")
    print("=" * 80)
    
    with progress_lock:
        print(f"n📊 PAGES:")
        print(f"  Processed:    {pages_processed:,}")
        print(f"  Saved:        {pages_saved:,}")
        print(f"  Duplicates:   {duplicates_found:,}")
        print(f"  Links found:  {links_discovered:,}")
        
        if pages_processed > 0:
            print(f"n  Duplicate rate: {(duplicates_found / pages_processed) * 100:.1f}%")
            print(f"  Save rate:      {(pages_saved / pages_processed) * 100:.1f}%")
    
    print(f"n❌ REJECTIONS:")
    with rejection_lock:
        total = sum(rejection_stats.values())
        print(f"  Total:        {total:,}n")
        
        for reason, count in rejection_stats.most_common(10):
            pct = (count / total * 100) if total > 0 else 0
            print(f"  {reason:30s} {count:8,} ({pct:5.1f}%)")
            if reason in rejection_samples:
                for sample_url in rejection_samples[reason][:2]:
                    print(f"    Sample: {sample_url}")
    
    print("n" + "=" * 80)
    save_processing_log()

import atexit
atexit.register(print_final_report)

def get_progress_stats():
    with progress_lock:
        return {
            'processed': pages_processed,
            'saved': pages_saved,
            'duplicates': duplicates_found,
            'discovered': links_discovered
        }