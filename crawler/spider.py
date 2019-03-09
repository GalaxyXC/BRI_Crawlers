import re
import json
import time
import datetime
from lxml import etree

import crawler.utils as utils

FILE_DIR = "crawler/"
FILE_DIR = "./"
KEYWORDS_FILE = "static/keywords.json"
WEBSITE_FILE = "static/websites.json"


class BeltRoadSpider:
    def __init__(self, keywords_json_file, keywords_checklist, start_date):
        with open(keywords_json_file, encoding="utf-8") as f:
            self.keywords_json = json.load(f)
        self.keywords_checklist = keywords_checklist
        self.start_date = start_date
        self.urls_to_return = []

    def crawl(self, mode_int, args):
        """
        call self.crawl(*) according to mode passed in
        :param mode_int: integer indicating which _crawl(*) to call
        :param args: arguments parsed to the designated crawler
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """
        if mode_int < 0:
            return ""

        if mode_int == 0:
            return self._crawl0(args)

        if mode_int == 1:
            return self._crawl1(args)

    def _crawl0(self, args):
        """
        mode 0:
        search page url: domain+link+"&cur_page="
        https://www.yidaiyilu.gov.cn/info/iList.jsp?cat_id=10122&cur_page=1
        in each news, href use relative link
        <a href="/xwzx/roll/80285.htm" target="_blank" title="XXX">XXX</a>

        :param args: name, domain, url_list, page_url_string, xpath's etc...
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """
        name, domain, link = args["name"], args["domain"], args["link"]
        page_url_prefix, page_url_suffix = args["page_url_prefix"], args["page_url_suffix"]
        min_page, max_page = args["min_page"], args["max_page"]
        url_xpath, title_xpath, document_xpath = args['url_xpath'], args['title_xpath'], args['document_xpath']
        print(f"Mode 0: Crawling {name}({domain}) with xpath (title | document).")

        urls_to_return = []
        page_urls = [domain+link+page_url_prefix+str(i)+page_url_suffix for i in range(min_page, max_page+1)][::-1]
        #page_urls = [domain+link+page_url_prefix+str(i)+page_url_suffix+'2']
        first_page = domain+link+page_url_prefix
        page_urls.append(first_page)

        # Get a list of url pages (news posts) from an index page (search result)
        for page in page_urls:
            document_urls = utils.get_urls_from_page(page, 0, url_xpath)
            if not document_urls:
                print("Fail to retrieve urls from post list. ")
            """
            TEMPORARY: re-crawl with new keywords
            with open("log.txt", 'r') as f:
                tmp = f.read()
                tmp_links = tmp.split('\n')
                document_urls = tmp_links[810:]
            """

            for document_url in document_urls:
                if "url_already_has_domain" in args:
                    document_html = utils.get_html_from_url(document_url)
                else:
                    document_html = utils.get_html_from_url(domain+document_url)
                print(f"pulling: {document_url}")

                if not document_html:
                    print(f"Fail to processed: {document_url}")
                    with open("log.txt", "a", encoding="utf-8") as f:
                        f.write("\n LOG: Fail to process: " + domain+document_url + "\n")
                    continue

                root = etree.HTML(document_html)
                try:
                    title = root.xpath(title_xpath)[0].text
                except Exception as et:
                    with open("log.txt", "a", encoding="utf-8") as f:
                        f.write(f"\n LOG: Exception: {et} while parsing Title {domain + document_url} \n")
                    title = ""

                try:
                    document_text = root.xpath(document_xpath)
                    document_string = " ".join([e.text for e in document_text if e.text])
                except Exception as ed:
                    with open("log.txt", "a", encoding="utf-8") as f:
                        f.write(f"\n LOG: Exception: {ed} while parsing Document {domain+document_url} \n")
                    continue

                found_log = "NOT "
                if self._contains_all_keywords(title+document_string):
                    urls_to_return.append(domain+document_url)
                    #urls_to_return.append(document_url)
                    #self.urls_to_return.append(domain+document_url)
                    found_log = ""

                print(f"Processed: {title}({document_url}), keyword {found_log}found. ")
                time.sleep(3)

            with open("log.txt", "a", encoding="utf-8") as f:
                f.write("\n LOG: Crawled: "+page+"\n")
                f.write(f"{datetime.datetime.now()}\n")
                f.write("\n".join(urls_to_return))

        return urls_to_return

    def _crawl1(self, args):
        """
        mode 1:
        search page url: domain+link+ (search keywords)"&cur_page="
        http://www.ceec.net.cn/jsearch/search.do?appid=1&ck=x&pagemode=result&pos=title%2Ccontent&q=%E7%85%A4%E7%94%B5&style=1&webid=40&&p=27
        in each news, href use relative link or full link
        <a href="/xwzx/roll/80285.htm" target="_blank" title="XXX">XXX</a>

        :param args: name, domain, url_list, page_url_string, xpath's etc...
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """

        name, domain, links = args["name"], args["domain"], args["link"]
        page_url_prefix, page_url_suffix = args["page_url_prefix"], args["page_url_suffix"]

        title_xpath, document_xpath = args['title_xpath'], args['document_xpath']
        print(f"Mode 1: Crawling {name}({domain}) with xpath (title | document).")

        # Create url queues
        if not args["read_from_file"]:
            document_urls = self.create_jobs(args)
        else:
            with open(args["read_from_file"], 'r') as f:
                document_urls = f.read().split('\n')

        # Consume url queues
        urls_to_return = []
        count = 0
        for document_url in document_urls:
            count += 1
            if args["href_pattern"] == "contain_domain":
                url = document_url
            elif args["href_pattern"] == "attached":
                url = domain + document_url
            elif args["href_pattern"] == "relative":
                pass

            print(f"[{count}] pulling : {url}")
            document_html = utils.get_html_from_url(url)

            if not document_html:
                print(f"Fail to processed: {document_url}")
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write("\n LOG: Fail to process: " + domain + document_url + "\n")
                continue

            root = etree.HTML(document_html)
            try:
                title = root.xpath(title_xpath)[0].text
            except Exception as et:
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write(f"\n LOG: Exception: {et} while parsing Title {domain + document_url} \n")
                title = ""

            try:
                document_text = root.xpath(document_xpath)
                document_string = " ".join([e.text for e in document_text if e.text])
            except Exception as ed:
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write(f"\n LOG: Exception: {ed} while parsing Document {domain + document_url} \n")
                continue

            found_log = "NOT "
            if self._contains_all_keywords(title + document_string):
                urls_to_return.append(domain + document_url)
                # urls_to_return.append(document_url)
                # self.urls_to_return.append(domain+document_url)
                found_log = ""
            print(f"Processed: {document_string[:12]}(length: {len(document_string)}).")
            print(f".. in {title}({document_url}), keyword {found_log}found. ")
            time.sleep(3)

            if count % 25 == 0:
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write(f"LOG: Crawled: {count} pages(Current: {document_url})\n {datetime.datetime.now()}\n")
                    f.write("\n".join(urls_to_return))
        return urls_to_return

    def create_jobs(self, args):
        name, domain, links = args["name"], args["domain"], args["link"]
        page_url_prefix, page_url_suffix = args["page_url_prefix"], args["page_url_suffix"]

        url_xpath = args['url_xpath']

        document_urls = set()
        for link in links:
            # generate a list of index pages
            min_page, max_page = link["min_page"], link["max_page"]
            page_urls = [domain + link["url"] + page_url_prefix + str(i) + page_url_suffix \
                         for i in range(min_page, max_page + 1)][::-1]
            if min_page == 1:
                first_page = domain + link["url"] + page_url_prefix
                page_urls.append(first_page)

            # Get a list of url pages (news posts) from an index page (search result)
            for page in page_urls:
                urls = utils.get_urls_from_page(page, 0, url_xpath)
                time.sleep(1.3)
                if not urls:
                    print(f"Fail to retrieve urls from post list({page}). ")
                document_urls.update(urls)
        document_urls = list(document_urls)
        file_name = name.split(' ')[0] + '_job_urls.txt'
        with open("log.txt", "a", encoding="utf-8") as f:
            f.write(f"\n LOG: tasks ({len(document_urls)} urls) stored in {file_name}.\n")
        print(f"{len(document_urls)} task urls stored in {file_name}.\n")
        with open("data/" + file_name, 'w') as f:
            f.write("\n".join(document_urls))




    def _contains_all_keywords(self, text):
        """
        :param text:
        :return: Boolean:
        """
        def _contains_keyword(keyword_group):
            for keyword in self.keywords_json[keyword_group]:
                if keyword in text:
                    return True
            return False

        for keyword_group in self.keywords_checklist:
            if not _contains_keyword(keyword_group):
                return False
        return True


if __name__ == '__main__':
    CHECKLIST = ["coal_keys", "countries", "action"]
    # CHECKLIST = ["coal_keys", "action"]  # test command
    spider = BeltRoadSpider(FILE_DIR + KEYWORDS_FILE, CHECKLIST, None)
    log = []

    with open(FILE_DIR + WEBSITE_FILE, encoding="utf-8") as f:
        websites = json.load(f)

    for website in websites["crawling"]:
        if website["crawl"] == 0:
            continue

        params = website["data"]
        params["read_from_file"] = "crawler/data/中国南方电网_job_urls.txt"
        # Override when read urls from file
        params['href_pattern'] = "contain_domain"
        log = spider.crawl(mode_int=params["crawl_mode_int"], args=params)

        with open("data/" + params["name"].split(" ")[0] + "output.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(log))

    print(f"LOG: Crawled {len(log)} links that contains all keywords in {CHECKLIST}. ")

