#!/usr/bin/env python3
"""Verify crawler completion and generate report"""

import shelve
import os
from collections import Counter
from urllib.parse import urlparse

def analyze_completion():
    print("=" * 80)
    print("CRAWLER COMPLETION REPORT")
    print("=" * 80)
    print()
    
    # Check frontier
    try:
        db = shelve.open('frontier.shelve', 'r')
        
        total_urls = len(db)
        completed_urls = []
        pending_urls = []
        
        status_by_domain = Counter()
        
        for url_hash, (url, completed) in db.items():
            domain = urlparse(url).netloc
            
            if completed:
                completed_urls.append(url)
                status_by_domain[domain] += 1
            else:
                pending_urls.append(url)
        
        db.close()
        
        # Print results
        print(f"📊 FRONTIER ANALYSIS")
        print("-" * 80)
        print(f"Total URLs discovered:    {total_urls:,}")
        print(f"Successfully completed:   {len(completed_urls):,}")
        print(f"Still pending:            {len(pending_urls):,}")
        print()
        
        # Completion status
        if len(pending_urls) == 0:
            print("✅ STATUS: CRAWL COMPLETED SUCCESSFULLY")
            print()
            print("All discovered URLs have been processed.")
            success = True
        else:
            print(f"⚠️  STATUS: CRAWL INCOMPLETE")
            print()
            print(f"{len(pending_urls)} URLs were not crawled:")
            for url in pending_urls[:10]:
                print(f"  - {url}")
            if len(pending_urls) > 10:
                print(f"  ... and {len(pending_urls) - 10} more")
            success = False
        
        print()
        print("-" * 80)
        
        # Domain breakdown
        print()
        print(f"📍 PAGES BY DOMAIN")
        print("-" * 80)
        for domain, count in status_by_domain.most_common():
            print(f"  {domain:40s} {count:,} pages")
        
        print()
        
    except Exception as e:
        print(f"❌ Error reading frontier: {e}")
        return False
    
    # Check saved data
    print()
    print(f"💾 SAVED DATA")
    print("-" * 80)
    
    if os.path.exists('crawl_data'):
        saved_files = len([f for f in os.listdir('crawl_data') if f.endswith('.json')])
        print(f"Pages with content saved:  {saved_files:,}")
        print(f"Storage used:              {get_dir_size('crawl_data')}")
    else:
        print("No saved data found")
    
    print()
    
    # Check logs
    print(f"📝 LOG ANALYSIS")
    print("-" * 80)
    
    if os.path.exists('Logs/Worker.log'):
        with open('Logs/Worker.log', 'r') as f:
            log_lines = f.readlines()
        
        downloads = len([l for l in log_lines if 'Downloaded' in l])
        errors = len([l for l in log_lines if 'ERROR' in l])
        status_200 = len([l for l in log_lines if 'status <200>' in l])
        status_404 = len([l for l in log_lines if 'status <404>' in l])
        status_403 = len([l for l in log_lines if 'status <403>' in l])
        
        print(f"Total download attempts:   {downloads:,}")
        print(f"Successful (200):          {status_200:,}")
        print(f"Not found (404):           {status_404:,}")
        print(f"Forbidden (403):           {status_403:,}")
        print(f"Errors logged:             {errors:,}")
    
    print()
    print("=" * 80)
    
    return success

def get_dir_size(path):
    """Get directory size"""
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    
    # Format size
    if total < 1024:
        return f"{total} B"
    elif total < 1024**2:
        return f"{total/1024:.1f} KB"
    elif total < 1024**3:
        return f"{total/1024**2:.1f} MB"
    else:
        return f"{total/1024**3:.1f} GB"

if __name__ == "__main__":
    success = analyze_completion()
    
    if success:
        print()
        print("🎉 Your crawl completed successfully!")
        print()
        print("Next steps:")
        print("  1. Run analysis: python3 analysis.py")
        print("  2. Generate report for assignment")
        print("  3. Optional: Clear data to save space")
    else:
        print()
        print("⚠️  Your crawl stopped early")
        print()
        print("To continue crawling:")
        print("  python3 launch.py  (without --restart)")