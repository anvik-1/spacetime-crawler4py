import os
import json
import shelve
from collections import Counter, defaultdict
from urllib.parse import urlparse, urldefrag

STOP_WORDS = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
    "that", "that's", "the", "their", "theirs", "them", "themselves", "then",
    "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
    "this", "those", "through", "to", "too", "under", "until", "up", "very", "was",
    "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
    "what's", "when", "when's", "where", "where's", "which", "while", "who",
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves"
])

def tokenize_words_list(words_list):
    """
    Tokenize a list of words (from JSON)
    """
    for word in words_list:
        word = word.lower()
        token = ""
        for char in word:
            try:
                if char.isascii() and char.isalnum():
                    token += char
                else:
                    if token:
                        yield token
                        token = ""
            except Exception:
                continue
        if token:
            yield token

def computeWordFrequencies(tokenIterator):
    counter = {}
    for token in tokenIterator:
        counter[token] = counter.get(token, 0) + 1
    return counter

def main():

    print("[1/4] Counting unique pages...")
    
    try:
        db = shelve.open('frontier.shelve', 'r')
        
        unique_urls = set()
        completed_urls = []
        
        for url_hash, (url, completed) in db.items():
            clean_url, _ = urldefrag(url)
            unique_urls.add(clean_url)
            
            if completed:
                completed_urls.append(clean_url)
        
        db.close()
        
        unique_completed = len(set(completed_urls))
        
        print(f"✓ Total unique pages: {unique_completed:,}")
        print()
    
    except Exception as e:
        print(f"Error reading frontier: {e}")
        #return
    
    print("[2/4] Analyzing page content...")
    
    if not os.path.exists('crawl_data'):
        print("Error: crawl_data directory not found!")
        return
    
    data_files = [f for f in os.listdir('crawl_data') if f.endswith('.json')]
    
    if not data_files:
        print("Error: No data files found in crawl_data/")
        return
    
    print(f"✓ Found {len(data_files)} saved pages")
    
    all_word_frequencies = Counter()
    
    # Track longest page
    longest_page = {
        'url': '',
        'word_count': 0
    }
    
    for i, filename in enumerate(data_files):
        if i % 500 == 0 and i > 0:
            print(f"  Progress: {i}/{len(data_files)}...")
        
        try:
            with open(os.path.join('crawl_data', filename), 'r') as f:
                data = json.load(f)
            
            words_list = data.get('words', [])
            
            tokens = list(tokenize_words_list(words_list))
            
            filtered_tokens = [
                t for t in tokens 
                if t not in STOP_WORDS  # Not a stop word
                and len(t) >= 3    # ← NEW: Additional validation
            ]            
            token_frequencies = computeWordFrequencies(iter(filtered_tokens))
            all_word_frequencies.update(token_frequencies)
            
            word_count = len(tokens) 
            if word_count > longest_page['word_count']:
                longest_page = {
                    'url': data['url'],
                    'word_count': word_count
                }
        
        except Exception as e:
            continue
    
    print(f"✓ Analysis complete")
    print()
    
    print("[3/4] Counting subdomains...")
    
    subdomains = defaultdict(set)  
    
    for url in unique_urls:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            if domain.endswith('.uci.edu') or domain == 'uci.edu':
                subdomains[domain].add(url)
        
        except Exception:
            continue
    
    subdomain_counts = {sub: len(urls) for sub, urls in subdomains.items()}
    
    print(f"✓ Found {len(subdomains)} subdomains")
    print()
    
    print("[4/4] Generating report...")
    
    report = []

    report.append("1. HOW MANY UNIQUE PAGES DID YOU FIND?")
    report.append("-" * 80)
    report.append(f"Answer: {unique_completed:,} unique pages")
    report.append("")
    
    report.append("2. WHAT IS THE LONGEST PAGE IN TERMS OF NUMBER OF WORDS?")
    report.append("-" * 80)
    report.append(f"Answer: {longest_page['url']}")
    report.append("")
    
    report.append("3. WHAT ARE THE 50 MOST COMMON WORDS?")
    report.append("-" * 80)
    report.append("Answer: (Stop words excluded, ordered by frequency)")
    report.append("")
    
    sorted_words = sorted(all_word_frequencies.items(), key=lambda x: x[1], reverse=True)
    
    for rank, (word, count) in enumerate(sorted_words[:50], 1):
        report.append(f"  {rank:2d}. {word:25s} {count:,}")
    
    report.append("")
    
    report.append("4. HOW MANY SUBDOMAINS IN UCI.EDU?")
    report.append("-" * 80)
    report.append(f"Answer: {len(subdomains)} subdomains")
    report.append("")
    report.append("Subdomain list (alphabetically ordered):")
    report.append("")
    
    for subdomain in sorted(subdomain_counts.keys()):
        count = subdomain_counts[subdomain]
        report.append(f"{subdomain}, {count}")
    

    report_text = '\n'.join(report)
    
    with open('REPORT.txt', 'w') as f:
        f.write(report_text)
    
    print()
    print(report_text)
    print()
    print("✅ Report saved to REPORT.txt")
    print()
    
    print("=" * 80)
    print("ADDITIONAL STATISTICS")
    print("=" * 80)
    print(f"Total unique words (excluding stop words): {len(all_word_frequencies):,}")
    print(f"Total word occurrences: {sum(all_word_frequencies.values()):,}")
    print(f"Average words per page: {sum(all_word_frequencies.values()) / len(data_files):.1f}")
    print("=" * 80)

if __name__ == "__main__":
    main()