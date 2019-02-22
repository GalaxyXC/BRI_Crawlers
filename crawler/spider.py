import re
import json
import requests
from lxml import etree

from crawler.utils import *

KEYWORDS_FILE = "crawler/static/keywords.json"
WEBSITE_FILE = "crawler/static/websites.json"


class BeltRoadSpider:
    def __init__(self, bri_country_list, action_words, start_date):
        self.bri_country_list = bri_country_list
        self.action_words = action_words
        self.start_date = start_date
        self.urls_to_return = []

    def crawl(self, domain, link, mode, mode_int, args):
        """
        call self.crawl(*) according to mode passed in
        :param domain: domain url
        :param link: typically url of a search page or list page under news channel
        :param mode: 'BY_PAGE or 'BY_DATE'
        :param mode_int: integer indicating which _crawl(*) to call
        :param args:
            if mode == 'BY_PAGE':
                args[0]: int: max. page corresponding to the starting page,
                args[1]: string: representing the page parameter string in url
            else:
                args: ?: max date
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """
        if mode_int == 0:
            return self._crawl0(domain, link, mode, args)

    def _crawl0(self, domain, link, mode, args):
        """
        mode 0:
        search page url: domain+link+"&cur_page="
        https://www.yidaiyilu.gov.cn/info/iList.jsp?cat_id=10122&cur_page=1
        in each news, href use relative link
        <a href="/xwzx/roll/80285.htm" target="_blank" title="XXX">XXX</a>

        :param domain: domain, typically url of a search page or list page under news channel
        :param link: typically url of a search page or list page under news channel
        :param mode: 'BY_PAGE or 'BY_DATE'
        :param args: if mode == 'BY_PAGE': max. page corresponding to the starting page, else None
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """
        urls_to_return = []
        max_page, page_param_string = args
        if mode == 'BY_PAGE':
            page_urls = [domain+link+'&'+page_param_string+'='+str(i) for i in range(max_page)]
            for page in page_urls:
                document_urls = get_urls_from_page(page, 0)
                for document_url in document_urls:
                    document_html = get_html_from_url(document_url)
                    document_text = etree.XML(document_html).path('body').text
                    if self._contains_BRI_country(document_text):
                        urls_to_return.append(document_url)
            return urls_to_return



    def _contains_BRI_country(self, text):
        """

        :param text:
        :return: Boolean:
        """

if __name__ == '__main__':
    with open(KEYWORDS_FILE, encoding="utf-8") as f:
        keywords = json.load(f)
    spider = BeltRoadSpider(keywords["countries"], keywords["actions"], None)

    with open(WEBSITE_FILE, encoding="utf-8") as f:
        websites = json.load(f)
    for data in websites["crawling"]["data"]:
        name, domain, link = data["name"], data["domain"], data["link"]
        crawl_mode_int, crawl_mode = data["crawl_mode_int"], data["mode"]
        print(f"Crawling: {name}({domain}), {crawl_mode}, and mode-{crawl_mode_int}")

        if crawl_mode == "BY_PAGE":
            max_page = data.get("max_page", 1)
            page_param_string = data["url_page_string"]
            spider.crawl(domain=domain, link=link,
                         mode=crawl_mode, mode_int=crawl_mode_int, args=[max_page, data["url_page_string"]])
        elif crawl_mode == "BY_DATE":
            max_date = data["max_date"]
            spider.crawl(domain=domain, link=link,
                         mode=crawl_mode, mode_int=crawl_mode_int, args=max_date)

    with open("output.txt", "w", encoding="utf-8") as f:
        f.write(spider.urls_to_return)
    print(f"LOG: Crawled {len(spider.urls_to_return)} links that contains BRI countries.")
