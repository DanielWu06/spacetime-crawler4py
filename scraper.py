import re
from urllib.parse import urlparse, urldefrag, urljoin
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
import logging


visited = set()
longest = ('',0)
common = {}
subdomains = {}
max_unique = 0

nltk.download("stopwords")
stop_words = set(stopwords.words("english"))

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    global visited
    global longest
    global common
    global subdomains
    global stop_words


    #DEBUG ONLY - SET A MAX UNIQUE PAGE LIMIT FOR CRAWLING AND MAX ITER LIMIT
    global max_unique
    max_unique = 5000
    if len(visited) > max_unique or iters > max_iters:
        logging.info("REACHED MAX UNIQUE, stopping crawler")
        return []
    
    if len(visited) % 100 == 0:
        logging.info("====UPDATE====")
        logging.info(f"Visited: {len(visited)} unique visited")
        logging.info(f"Subdomains: {len(subdomains)} subdomains seen")
        for k, v in sorted(subdomains.items(), key=lambda x: len(x[1]), reverse=True)[:3]:
            logging.info(f"Top domains: {k} -> {len(v)} pages")
        logging.info(f"Words found: {len(common)}")
        logging.info("===============")


    #Defrag
    clean_url, toss = urldefrag(url)

    res = list()

    if not (resp.status == 200 and resp.raw_response.content):
        return []
    

    #UNIQUE
    if clean_url not in visited:
        visited.add(clean_url)
    else:
        return []

    #SUBDOMAIN
    host = urlparse(url).netloc
    subdomains.setdefault(host, set()).add(clean_url)

    #WORDS
    content_soup = BeautifulSoup(resp.raw_response.content, "lxml")
    for tag in content_soup(["script", "style", "noscript"]):
        tag.decompose()

    text = content_soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    words = re.findall(r"[a-zA-Z']+", text.lower())
    words = [w for w in words if w not in stop_words]
    for w in words:
        common[w] = common.get(w,0) + 1

    #LONGEST
    len_page = len(words)
    if len_page > longest[1]:
        longest = (clean_url, len_page)

    #SUBDOMAIN done in checks

    for a in content_soup.find_all('a', href = True):
        href_url = a['href']

        full_url = urljoin(url, href_url)
        clean_full_url, toss = urldefrag(full_url)

        if is_valid(clean_full_url):
            res.append(clean_full_url)


    return res

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    try:
        parsed = urlparse(url)

        if parsed.scheme not in set(["http", "https"]):
            return False
        
        host = parsed.netloc
        if not (
            host.endswith(".ics.uci.edu") or
            host.endswith(".cs.uci.edu") or
            host.endswith(".informatics.uci.edu") or
            host.endswith(".stat.uci.edu")
        ):
            return False
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def report():
    logging.info("==========REPORT==========")
    logging.info(f"Unique pages: {len(visited)}")

    logging.info(f"Longest page: {longest[0]}, {longest[1]}")

    for word, count in sorted(common.items(), key=lambda x: -x[1])[:50]:
        logging.info(f"{word}, {count}")

    for k in sorted(subdomains):
        logging.info(f"{k}, {len(subdomains[k])}")