
def is_duplicate_content_fast(text_content):
    """Optimized duplicate detection"""
    if not text_content or len(text_content.strip()) < 50:
        return True
    
    # Exact duplicate check
    content_hash = hashlib.md5(text_content.encode('utf-8', errors='ignore')).hexdigest()
    
    with content_lock:
        if content_hash in seen_exact_hashes:
            return True
        seen_exact_hashes.add(content_hash)
    
    # Near-duplicate check (simhash)
    content_simhash = compute_simhash(text_content)
    
    with content_lock:
        for seen_hash, _ in seen_simhashes:
            if hamming_distance(content_simhash, seen_hash) <= SIMHASH_THRESHOLD:
                return True
        
        seen_simhashes.append((content_simhash, len(seen_exact_hashes)))
    
    return False

def hamming_distance(hash1, hash2):
    """Calculate Hamming distance"""
    return bin(hash1 ^ hash2).count('1')

def compute_simhash(text, hash_bits=64):
    """Compute simhash fingerprint"""
    words = text.split()
    
    if len(words) < 3:
        return hash(text) & ((1 << hash_bits) - 1)
    
    if len(words) > 500:
        step = len(words) // 250
        tokens = [' '.join(words[i:i+3]) for i in range(0, len(words) - 2, step)]
    else:
        tokens = [' '.join(words[i:i+3]) for i in range(len(words) - 2)]
    
    if not tokens:
        return 0
    
    vector = [0] * hash_bits
    
    for token in tokens:
        token_hash = hash(token) & ((1 << hash_bits) - 1)
        for i in range(hash_bits):
            if token_hash & (1 << i):
                vector[i] += 1
            else:
                vector[i] -= 1
    
    fingerprint = 0
    for i in range(hash_bits):
        if vector[i] > 0:
            fingerprint |= (1 << i)
    
    return fingerprint

