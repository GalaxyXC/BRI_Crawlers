import requests
from lxml import etree

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " \
             "AppleWebKit/537.36 (KHTML, like Gecko) " \
             "Chrome/72.0.3626.109 Safari/537.36"


def get_urls_from_page(page_url, mode):
    """
    Calls get_urls_from_page(*) from utils.py
    :param page_url: url of page
    :param mode: int
    :return: List[string], a list of urls
    """
    if mode == 0:
        return _get_urls_from_page0(page_url)


def _get_urls_from_page0(page_url):
    # parsing YiDaiYiLuWang's news list
    html = get_html_from_url(page_url)
    root = etree.XML(html)
    li_elements = root.xpath()  # TODO

    urls_to_return = []
    for li_element in li_elements:
        urls_to_return.append(li_element.xpath())  # TODO
    return urls_to_return


def get_post_date_from_document(document, mode):
        """
        Calls post_date_from_document(*) from utils.py
        :param document: HTML document
        :return: Time stamp in string YY-MM-DD
        """
        pass


def get_html_from_url(url):
    return requests.get(url=url, headers={'user-agent':USER_AGENT}, params=None)