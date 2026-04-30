from threading import Thread, Lock

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

from urllib.parse import urlparse
domain_locks = {}
last_domain_access ={}
registry_lock = Lock()

def get_domain(url):
    host = urlparse(url).netloc
    parts = host.split(".")
    if len(parts) >= 3:
        return ".".join(parts[-3:])
    return host

def politeness_enforcement(domain, delay):
    with registry_lock:
        if domain not in domain_locks:
            domain_locks[domain] = Lock()
            last_domain_access[domain] = 0.0

    domain_locks[domain].acquire()
    try:
        elapsed_time = time.time() - last_domain_access[domain]
        waiting_time = delay - elapsed_time
        print(f"waiting time: {waiting_time}")
        if waiting_time > 0:
            time.sleep(waiting_time)
        last_domain_access[domain] = time.time()

    finally:
        domain_locks[domain].release()


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.worker_id = worker_id
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                if self.worker_id == 0:
                    scraper.report(self.logger)
                break

            domain = get_domain(tbd_url)
            #  instead of sleep at the end for multithreading
            politeness_enforcement(domain, self.config.time_delay)

            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            # time.sleep(self.config.time_delay)
