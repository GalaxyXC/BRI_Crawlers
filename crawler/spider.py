import json
import time
import datetime
from lxml import etree

from crawler.utils import *

KEYWORDS_FILE = "./static/keywords.json"
WEBSITE_FILE = "./static/websites.json"


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

        :param domain: domain url
        :param link: typically url of a search page or list page under news channel
        :param mode: 'BY_PAGE or 'BY_DATE'
        :param args:
            if mode == 'BY_PAGE':
                args[0]: int: max. page corresponding to the starting page,
                args[1]: string: representing the page parameter string in url
            else:
                args: ?: max date
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """
        urls_to_return = []
        max_page, page_param_string = args
        if mode == 'BY_PAGE':
            page_urls = [domain+link+'&'+page_param_string+'='+str(i) for i in range(1,max_page+1)][::-1]
            #page_urls = [domain+link+'&'+page_param_string+'='+str(3)]
            for page in page_urls:
                document_urls = get_urls_from_page(page, 0)
                for document_url in document_urls:
                    time.sleep(4)
                    document_html = get_html_from_url(domain+document_url)
                    if not document_html:
                        print(f"Fail to processed: {document_url}")
                        with open("log.txt", "a", encoding="utf-8") as f:
                            f.write("\n LOG: Fail to process: " + domain+document_url + "\n")
                        continue

                    root = etree.HTML(document_html)
                    try:
                        title = root.xpath("//div/h1[@class='main_content_title']")[0].text
                        document_text = root.xpath("//div[@class='content']/div/p")
                        document_string = " ".join([e.text for e in document_text])
                    except Exception as e:
                        with open("log.txt", "a", encoding="utf-8") as f:
                            f.write(f"\n LOG: Exception: {e} while parsing {domain+document_url} \n")
                        continue

                    found_log = "NOT "
                    if self._contains_BRI_country(title) or self._contains_BRI_country(document_string):
                        urls_to_return.append(domain+document_url)
                        #self.urls_to_return.append(domain+document_url)
                        found_log = ""
                    print(f"Processed: {title}({document_url}), keyword {found_log}found. ")
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write("\n LOG: Crawled: "+page+"\n")
                    f.write(f"{datetime.datetime.now()}\n")
                    f.write("\n".join(urls_to_return))

        return urls_to_return

    def _contains_BRI_country(self, text):
        """
        :param text:
        :return: Boolean:
        """
        found_action = False
        for action in self.action_words:
            if action in text:
                found_action = True
                break

        if found_action:
            for country in self.bri_country_list:
                if country in text:
                    return True
            return False
        else:
            return False

if __name__ == '__main__':
    with open(KEYWORDS_FILE, encoding="utf-8") as f:
        keywords = json.load(f)
    spider = BeltRoadSpider(keywords["countries"], keywords["action"], None)

    with open(WEBSITE_FILE, encoding="utf-8") as f:
        websites = json.load(f)
    for website in websites["crawling"]:
        data = website["data"]
        name, domain, link = data["name"], data["domain"], data["link"]
        crawl_mode_int, crawl_mode = data["crawl_mode_int"], data["mode"]
        print(f"Crawling: {name}({domain}), {crawl_mode}, and mode-{crawl_mode_int}")

        if crawl_mode == "BY_PAGE":
            max_page = 1 if not data["max_page"] else data["max_page"]
            log = spider.crawl(domain=domain, link=link,
                               mode=crawl_mode, mode_int=crawl_mode_int, args=[max_page, data["url_page_string"]])
        elif crawl_mode == "BY_DATE":
            max_date = data["max_date"]
            log = spider.crawl(domain=domain, link=link,
                               mode=crawl_mode, mode_int=crawl_mode_int, args=max_date)

    with open("output.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    print(f"LOG: Crawled {len(log)} links that contains BRI countries.")
