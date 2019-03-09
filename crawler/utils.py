import re
import requests
from lxml import etree

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
             "AppleWebKit/537.36 (KHTML, like Gecko) " \
             "Chrome/72.0.3626.109 Safari/537.36"


def get_urls_from_page(page_url, mode, args):
    """
    Calls get_urls_from_page(*)
    :param page_url: url of page
    :param mode: int
    :return: List[string], a list of urls
    """
    if mode == 0:
        return _get_urls_from_page0(page_url, url_xpath=args)
    elif mode == 1:
        return _get_urls_from_page0(page_url, url_xpath=args)


def _get_urls_from_page0(page_url, url_xpath):
    # parsing YiDaiYiLuWang's news list
    html = get_html_from_url(page_url)
    if not html:
        return []

    root = etree.HTML(html)
    url_elements = root.xpath(url_xpath)
    return [elem.get("href") for elem in url_elements]



def _get_urls_from_page0(page_url, url_xpath):
    # parsing YiDaiYiLuWang's news list
    html = get_html_from_url(page_url)
    if not html:
        return []

    root = etree.HTML(html)
    url_elements = root.xpath(url_xpath)
    return [elem.get("href") for elem in url_elements]




def get_post_date_from_document(document, mode):
        """
        Calls post_date_from_document(*) from utils.py
        :param document: HTML document
        :return: Time stamp in string YY-MM-DD
        """
        pass


def get_html_from_url(url):
    r = requests.get(url=url, headers={'user-agent':USER_AGENT}, params=None)
    if r.status_code == 200:
        r.encoding = 'UTF-8'
        return r.text
    else:
        return ""

def remove_domain_prefix(path, file, domain):
    with open(path+file, 'r') as f:
        text = f.read()
    lines = text.split('\n')
    ret = []
    for line in lines:
        ret.append(line.replace(domain, '', 1))
    new_text = "\n".join(ret)
    with open(path+"new_"+file, 'w') as f:
        f.write(new_text)

def merge_urls(file1, file2):
    with open(file1, 'r') as f:
        text = f.read()
    lines1 = text.split('\n')
    with open(file2, 'r') as f:
        text = f.read()
    lines2 = text.split('\n')
    unique = set(lines1)
    unique.update(lines2)
    with open("unique_urls.txt", 'w') as f:
        f.write("\n".join(unique))

def add_domain_by_year(file, year_prefix, year_suffix):
    with open(file, 'r') as f:
        urls = f.read().split('\n')

    ret = []
    for u in urls:
        year = re.search('201[0-9]', u).group(0)
        url = year_prefix + year + year_suffix + u.split('/', 1)[1]
        ret.append(url)

    with open("crawler/new_urls.txt", 'w') as f:
        f.write("\n".join(ret))







def parse_year(file):
    return
