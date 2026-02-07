#!/usr/bin/env python3
"""Check if seed domains were actually crawled"""

import shelve
from urllib.parse import urlparse
from collections import defaultdict

db = shelve.open('frontier.shelve', 'r')

# Count by domain
domain_counts = defaultdict(lambda: {'total': 0, 'completed': 0})

for url_hash, (url, completed) in db.items():
    domain = urlparse(url).netloc.lower()
    domain_counts[domain]['total'] += 1
    if completed:
        domain_counts[domain]['completed'] += 1

db.close()

print("=" * 80)
print("DOMAIN CRAWL STATUS")
print("=" * 80)
print()

# Check seed domains
seed_domains = [
    'www.ics.uci.edu',
    'www.cs.uci.edu',
    'www.informatics.uci.edu',
    'www.stat.uci.edu'
]

for domain in seed_domains:
    if domain in domain_counts:
        stats = domain_counts[domain]
        print(f"✓ {domain:40s} {stats['completed']:5d}/{stats['total']:5d}")
    else:
        print(f"❌ {domain:40s} NOT FOUND IN FRONTIER!")

print()
print("All domains found:")
for domain in sorted(domain_counts.keys()):
    if any(seed in domain for seed in ['cs.uci.edu', 'informatics.uci.edu', 'stat.uci.edu']):
        stats = domain_counts[domain]
        print(f"   {domain:40s} {stats['completed']:5d}/{stats['total']:5d}")