import os
import shelve
from threading import RLock
from queue import Queue, Empty
from urllib.parse import urlparse
from time import time

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        
        # Thread safety
        self.lock = RLock()
        
        # Per-domain queues and politeness tracking
        self.domain_queues = {}  # domain -> Queue of URLs
        self.last_access_time = {}  # domain -> timestamp of last access
        
        # URL tracking
        self.urls_seen = set()  # For quick duplicate checking
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        '''This function can be overridden for alternate saving techniques.'''
        total_count = len(self.save)
        tbd_count = 0
        
        with self.lock:
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    self._add_to_domain_queue(url)
                    self.urls_seen.add(normalize(url))
                    tbd_count += 1
        
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def _get_domain(self, url):
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return "unknown"
    
    def _add_to_domain_queue(self, url):
        """Add URL to its domain's queue (must be called with lock held)"""
        domain = self._get_domain(url)
        
        if domain not in self.domain_queues:
            self.domain_queues[domain] = Queue()
        
        self.domain_queues[domain].put(url)
    
    def _get_total_tbd_count(self):
        """Get total number of URLs to be downloaded"""
        count = 0
        for queue in self.domain_queues.values():
            count += queue.qsize()
        return count

    def get_tbd_url(self):
        """
        Get one URL that is ready to be downloaded (respecting politeness).
        Returns None if no URL is available or all domains need to wait.
        """
        with self.lock:
            current_time = time()
            politeness_delay = self.config.time_delay  # 0.5 seconds
            
            # Find domains that have URLs and are ready to be accessed
            ready_domains = []
            
            for domain, queue in self.domain_queues.items():
                if queue.empty():
                    continue
                
                last_time = self.last_access_time.get(domain, 0)
                time_since_last = current_time - last_time
                
                # Check if enough time has passed since last access to this domain
                if time_since_last >= politeness_delay:
                    ready_domains.append((domain, queue))
            
            # If no domains are ready, check if we're truly done
            if not ready_domains:
                total_remaining = self._get_total_tbd_count()
                if total_remaining == 0:
                    return None  # Truly done
                # URLs exist but all domains need to wait
                return None  # Caller should retry after short sleep
            
            # Pick first ready domain
            domain, queue = ready_domains[0]
            
            try:
                url = queue.get_nowait()
                
                # Update last access time for this domain
                self.last_access_time[domain] = current_time
                
                return url
            
            except Empty:
                return None

    def add_url(self, url):
        """Add URL to frontier (with duplicate checking)"""
        url = normalize(url)
        urlhash = get_urlhash(url)
        
        with self.lock:
            # Check if already seen (in-memory check, faster than shelve)
            if url in self.urls_seen:
                return
            
            # Check if in save file
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                
                # Add to domain queue
                self._add_to_domain_queue(url)
                self.urls_seen.add(url)
    
    def mark_url_complete(self, url):
        """Mark URL as completed"""
        urlhash = get_urlhash(url)
        
        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            
            self.save[urlhash] = (url, True)
            self.save.sync()