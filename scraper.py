import re
from urllib.parse import urlparse, urldefrag, urljoin, parse_qsl
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
import logging
from threading import Lock

mt_scraper_lock = Lock()


visited = set()
longest = ('',0)
common = {}
subdomains = {}
max_unique = 0
fingerprints = []

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
    global fingerprints


    #DEBUG ONLY - SET A MAX UNIQUE PAGE LIMIT FOR CRAWLING AND MAX ITER LIMIT
    # global max_unique
    # max_unique = 500000
    # if len(visited) > max_unique:
    #     logging.info("REACHED MAX UNIQUE, stopping crawler")
    #     return []
    
    '''
    if len(visited) % 100 == 0:
        logging.info("====UPDATE====")
        logging.info(f"Visited: {len(visited)} unique visited")
        logging.info(f"Subdomains: {len(subdomains)} subdomains seen")
        for k, v in sorted(subdomains.items(), key=lambda x: len(x[1]), reverse=True)[:3]:
            logging.info(f"Top domains: {k} -> {len(v)} pages")
        logging.info(f"Words found: {len(common)}")
        logging.info("===============")
    '''


    #Defrag
    # https://vision.ics.uci.edu/example#abcde -> https://vision.ics.uci.edu/example, abcde
    clean_url, toss = urldefrag(url)

    res = list()

    if not (resp.status == 200 and resp.raw_response.content):
        return []
    #empty or large files anything more than 5MB
    if len(resp.raw_response.content) == 0 or len(resp.raw_response.content) >= 5000000:
        return []
    
    #UNIQUE
    with mt_scraper_lock:
        if clean_url in visited:
            return []
        else:
            visited.add(clean_url)

    content_soup = BeautifulSoup(resp.raw_response.content, "lxml")
    for tag in content_soup(["script", "style", "noscript"]):
        tag.decompose()

    text = content_soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    words = re.findall(r"[a-zA-Z']+", text.lower())
    words = [w for w in words if w not in stop_words and len(w) > 2] # dont add any words len < 2
    #low content
    if not words or len(words) < 50:
        return []

    #Simhash check
    
    fingerprint = simhash(words)
    with mt_scraper_lock:
        if(fingerprint in fingerprints):
            #exact copy
            return []

        for other in fingerprints:
            #check similarity

            diff = bin(other^fingerprint).count("1")
            similarity = 1 - (diff/64)
            if similarity >= 0.8:
                return []

        #Log new fingerprint
        fingerprints.append(fingerprint)

    #All checks passed, update info
    #WORDS
    with mt_scraper_lock:
        for w in words:
            common[w] = common.get(w,0) + 1
        #LONGEST
        len_page = len(words)
        if len_page > longest[1]:
            longest = (clean_url, len_page)

        #SUBDOMAIN
        host = urlparse(clean_url).netloc
        subdomains.setdefault(host, set()).add(clean_url)


    for a in content_soup.find_all('a', href = True):
        href_url = a['href']

        try:
            full_url = urljoin(url, href_url)
            clean_full_url, toss = urldefrag(full_url)

            if is_valid(clean_full_url):
                res.append(clean_full_url)
        except Exception:
            continue



    return res

def simhash(words) -> int:
    #Returns our fingerprint for the simhash of the page
    false_flags = {'home','login','logout','menu','search','copyright','credit','privacy','terms','faculty','department','news','services'}
    new_words = [w for w in words if not w.isdigit() and len(w)>2 and w not in false_flags]

    freq = {}
    for w in new_words:
        freq[w] = freq.get(w, 0) + 1

    v = [0] * 64
    for w, weight in freq.items():
        word_hash = hash(w)

        for i in range(64):
            #rshift for all 64 bits
            bit = (word_hash>>i) & 1

            #add weight if component if hash is 1, subtract otherwise
            v[i] += weight if bit == 1 else (-1 * weight)

    fingerprint = 0
    for i in range(64):
        if v[i] > 0:
            fingerprint |= (1<<i)

    return fingerprint


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
        
        #Avoid query traps
        params = parse_qsl(parsed.query)
        query_traps = {'do','idx','diff','export','year','month','day','date','sid','sessionid', 'history', 'version'}
        #Too many params
        if len(params) > 4:
            return False
        #Found a likely trap OR Param too long
        if any((a in query_traps) or (len(a)>40) for a, b in params):
            return False
        numeric_count = sum(a.isdigit() for a, b in params)
        if numeric_count >= 2:
            return False
        
        #ISO datetime matching
        if re.search(r'\d{4}-\d{2}-\d{2}T\d{2}', parsed.query):
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

def report(logger):
    logger.info("==========REPORT==========")
    logger.info(f"Unique pages: {len(visited)}")

    logger.info(f"Longest page: {longest[0]}, {longest[1]}")

    for word, count in sorted(common.items(), key=lambda x: -x[1])[:50]:
        logger.info(f"{word}, {count}")

    for k in sorted(subdomains):
        logger.info(f"{k}, {len(subdomains[k])}")
