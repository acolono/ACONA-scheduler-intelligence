import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import sys

sys.setrecursionlimit(10000)


def acona_get_ua():
    # randomizes the User-Agent string to get around servers that throw errors if you try to crawl with the default User-Agent
    uastrings = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",\
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",\
                "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0",\
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36"\
                ]

    return random.choice(uastrings)


def acona_parse_url(url):
    #returns the content, BeautifulSoup-parsed DOM, and content type for a URL.
    headers = {'User-Agent': acona_get_ua()}
    content = None

    try:
        response = requests.get(url, headers=headers)
        ct = response.headers['Content-Type'].lower().strip()

        if 'text/html' in ct:
            content = response.content
            soup = BeautifulSoup(content, "lxml")
        else:
            content = response.content
            soup = None

    except Exception as e:
        print("Error:", str(e))

    return content, soup, ct


def acona_parse_internal_links(soup, current_page):
    #grabs the internal links for a web page.
    return [a['href'].lower().strip() for a in soup.find_all('a', href=True) if urlparse(a['href']).netloc == urlparse(current_page).netloc or urlparse(a['href']).netloc == '']

def acona_parse_external_links(soup, current_page):
    #grabs the external links for a web page.
    return [a['href'].lower().strip() for a in soup.find_all('a', href=True) if urlparse(a['href']).netloc != '' and urlparse(a['href']).netloc != urlparse(current_page).netloc]