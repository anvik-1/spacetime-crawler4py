#!/usr/bin/env python3
"""
Enhanced log-based progress monitor with ETA estimation
"""

import os
import time
import shelve
from datetime import datetime, timedelta
from collections import deque

def format_time(seconds):
    """Format seconds into readable time"""
    if seconds < 0:
        return "calculating..."
    elif seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"

def try_read_frontier():
    """Try to read frontier database with timeout"""
    try:
        db = shelve.open('frontier.shelve', flag='r')
        
        total = 0
        completed = 0
        
        # Quick count with timeout protection
        for url_hash, (url, is_completed) in db.items():
            total += 1
            if is_completed:
                completed += 1
            
            # Don't spend too long reading
            if total > 10000 and total % 1000 == 0:
                break
        
        pending = total - completed
        db.close()
        
        return {
            'total': total,
            'completed': completed,
            'pending': pending,
            'accessible': True
        }
    
    except Exception:
        return {'accessible': False}

def estimate_progress(downloads_count, elapsed, frontier_data):
    """Estimate completion time based on available data"""
    
    # Calculate current rate
    current_rate = downloads_count / elapsed if elapsed > 0 else 0
    
    if frontier_data.get('accessible'):
        # We have accurate data from frontier
        pending = frontier_data['pending']
        total = frontier_data['total']
        completed = frontier_data['completed']
        
        if current_rate > 0 and pending > 0:
            eta_seconds = pending / current_rate
            progress_pct = (completed / total * 100) if total > 0 else 0
        else:
            eta_seconds = -1
            progress_pct = 0
        
        return {
            'eta_seconds': eta_seconds,
            'progress_pct': progress_pct,
            'pending': pending,
            'total': total,
            'method': 'accurate'
        }
    else:
        # Estimate based on crawl behavior
        # Most crawls discover 2-10x more URLs than initially seeded
        # We'll use a conservative estimate
        
        # Check if crawler is still discovering new URLs
        # by looking at recent activity
        estimated_total = downloads_count * 3  # Conservative 3x multiplier
        estimated_pending = estimated_total - downloads_count
        
        if current_rate > 0:
            eta_seconds = estimated_pending / current_rate
            progress_pct = (downloads_count / estimated_total * 100)
        else:
            eta_seconds = -1
            progress_pct = 0
        
        return {
            'eta_seconds': eta_seconds,
            'progress_pct': progress_pct,
            'pending': estimated_pending,
            'total': estimated_total,
            'method': 'estimated'
        }

def monitor():
    start_time = time.time()
    
    # Track rate over time for better ETA
    rate_history = deque(maxlen=10)  # Last 10 measurements
    last_count = 0
    last_check = start_time
    
    print("Starting log monitor...")
    print("Press Ctrl+C to stop\n")
    time.sleep(2)
    
    frontier_check_interval = 30  # Try to read frontier every 30 seconds
    last_frontier_check = 0
    frontier_data = {'accessible': False}
    
    while True:
        try:
            
            current_time = time.time()
            elapsed = current_time - start_time
            
            # Parse logs
            downloads = []
            errors = 0
            
            if os.path.exists('Logs/Worker.log'):
                with open('Logs/Worker.log', 'r') as f:
                    for line in f:
                        if 'Downloaded' in line:
                            downloads.append(line)
                        elif 'ERROR' in line:
                            errors += 1
            
            # Count saved files
            saved = 0
            if os.path.exists('crawl_data'):
                saved = len([f for f in os.listdir('crawl_data') if f.endswith('.json')])
            
            downloads_count = len(downloads)
            
            # Calculate instantaneous rate
            time_since_check = current_time - last_check
            if time_since_check >= 5:  # Update every 5 seconds
                count_diff = downloads_count - last_count
                instant_rate = count_diff / time_since_check if time_since_check > 0 else 0
                rate_history.append(instant_rate)
                last_count = downloads_count
                last_check = current_time
            
            # Calculate average rate from history
            if rate_history:
                avg_rate = sum(rate_history) / len(rate_history)
            else:
                avg_rate = downloads_count / elapsed if elapsed > 0 else 0
            
            # Try to read frontier periodically
            if current_time - last_frontier_check > frontier_check_interval:
                frontier_data = try_read_frontier()
                last_frontier_check = current_time
            
            # Estimate progress and ETA
            progress_info = estimate_progress(downloads_count, elapsed, frontier_data)
            
            # Display
            print("=" * 80)
            print("🕷️  WEB CRAWLER MONITOR")
            print("=" * 80)
            print()
            
            print(f"⏱️  Running Time:     {format_time(elapsed)}")
            print(f"📥 Total Downloads:   {downloads_count:,}")
            print(f"💾 Pages Saved:       {saved:,}")
            print(f"❌ Errors:            {errors}")
            print()
            
            print(f"🚀 Current Rate:      {avg_rate:.2f} pages/sec")
            
            
            # ETA
            if progress_info['eta_seconds'] > 0:
                eta_str = format_time(progress_info['eta_seconds'])
                completion_time = datetime.now() + timedelta(seconds=progress_info['eta_seconds'])
                print(f"⏳ Est. Time Left:    {eta_str}")
                print(f"🎯 Est. Completion:   {completion_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"⏳ Est. Time Left:    calculating...")
            
            print()
            
            # Status info
            if frontier_data.get('accessible'):
                print(f"📈 URLs Discovered:   {progress_info['total']:,}")
                print(f"✅ Completed:         {frontier_data.get('completed', 0):,}")
                print(f"⏰ Pending:           {progress_info['pending']:,}")
                print(f"ℹ️  Data Source:       Frontier Database (accurate)")
            else:
                print(f"📈 Est. Total URLs:   ~{progress_info['total']:,}")
                print(f"⏰ Est. Pending:      ~{progress_info['pending']:,}")
                print(f"ℹ️  Data Source:       Estimated (frontier locked)")
            
            print()
            
            # Recent activity
            print("📝 Last 5 Downloads:")
            print("-" * 80)
            for line in downloads[-5:]:
                parts = line.split('Downloaded')
                if len(parts) > 1:
                    url = parts[1].split(',')[0].strip()
                    status = parts[1].split('status')[1].split(',')[0].strip() if 'status' in parts[1] else '?'
                    print(f"  {status} {url}")
            
            print()
            print("=" * 80)
            print(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
            print("Refreshing every 3 seconds... (Press Ctrl+C to stop)")
            
            time.sleep(3)
        
        except KeyboardInterrupt:
            print("\n\n✅ Monitoring stopped")
            print(f"\nFinal Stats:")
            print(f"  Total runtime: {format_time(time.time() - start_time)}")
            print(f"  Pages downloaded: {downloads_count:,}")
            print(f"  Average rate: {downloads_count / (time.time() - start_time):.2f} pages/sec")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(3)

if __name__ == "__main__":
    monitor()