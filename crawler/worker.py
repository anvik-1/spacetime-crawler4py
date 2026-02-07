from threading import Thread
from inspect import getsource
from time import sleep, time
from utils.download import download
from utils import get_logger
import scraper
import traceback

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.worker_id = worker_id
        
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, \
            "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, \
            "Do not use urllib.request in scraper.py"
        
        super().__init__(daemon=True)
        
    def run(self):
        self.logger.info(f"Worker-{self.worker_id} started")
        
        consecutive_none_count = 0
        max_consecutive_none = 100  # ← INCREASED from 20
        last_successful_download = time()
        max_idle_time = 30  # ← Stop after 30 seconds of no progress
        
        while True:
            try:
                tbd_url = self.frontier.get_tbd_url()
                
                if not tbd_url:
                    consecutive_none_count += 1
                    
                    # Check if we've been idle too long
                    idle_time = time() - last_successful_download
                    
                    if consecutive_none_count >= max_consecutive_none and idle_time > max_idle_time:
                        # Double-check frontier is actually empty
                        total_remaining = self.frontier._get_total_tbd_count()
                        
                        if total_remaining == 0:
                            self.logger.info("Frontier is empty. Stopping Crawler.")
                            break
                        else:
                            self.logger.info(
                                f"Waiting for politeness delay... "
                                f"({total_remaining} URLs remaining)"
                            )
                            consecutive_none_count = 0  # Reset counter
                            sleep(1)  # Longer wait when stuck
                            continue
                    
                    # Normal wait
                    sleep(0.1)
                    continue
                
                # Got a URL - reset counters
                consecutive_none_count = 0
                last_successful_download = time()
                
                # Download the page
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                
                # Scrape for new URLs
                try:
                    scraped_urls = scraper.scraper(tbd_url, resp)
                except Exception as e:
                    self.logger.error(f"Scraper error for {tbd_url}: {e}")
                    scraped_urls = []
                
                # Add new URLs to frontier
                for scraped_url in scraped_urls:
                    try:
                        self.frontier.add_url(scraped_url)
                    except Exception as e:
                        self.logger.error(f"Error adding URL {scraped_url}: {e}")
                
                # Mark as complete
                try:
                    self.frontier.mark_url_complete(tbd_url)
                except Exception as e:
                    self.logger.error(f"Error marking complete {tbd_url}: {e}")
                
                self.logger.info(
                    f"Processed {tbd_url}, found {len(scraped_urls)} valid links")
                
                # Small delay to reduce lock contention
                sleep(0.05)
            
            except Exception as e:
                self.logger.error(
                    f"Worker-{self.worker_id}: Unexpected error: {e}")
                self.logger.error(traceback.format_exc())
                
                # Mark URL as complete to avoid infinite retry
                if 'tbd_url' in locals():
                    try:
                        self.frontier.mark_url_complete(tbd_url)
                    except:
                        pass
        
        self.logger.info(f"Worker-{self.worker_id} stopped")