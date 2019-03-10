import re
import json
import time
import datetime as dt
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

        if mode_int == 2:
            return self._crawl2(args)

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
                    document_html = utils.get_html_from_url(document_url, args)
                else:
                    document_html = utils.get_html_from_url(domain+document_url, args)
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
                result, matched = self._contains_all_keywords(title + document_string)
                if result:
                    urls_to_return.append(domain+document_url)
                    #urls_to_return.append(document_url)
                    #self.urls_to_return.append(domain+document_url)
                    found_log = ""

                print(f"Processed: {title}({document_url}), keyword {found_log}found.{matched}")
                time.sleep(3)

            utils.log(f"\n LOG: Crawled: {page}\n {dt.datetime.now()}\n")
            utils.log("\n".join(urls_to_return))

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
        utils.log(f"Mode 1: Crawling {name}({domain}) with xpath (title | document); {dt.datetime.now}")

        # Create url queues
        if "read_from_file" not in args:
            jobs = self.create_jobs(args)
        else:
            with open(args["read_from_file"], 'r') as f:
                jobs = f.read().split('\n')

        # Consume url queues
        return self.consume_jobs_xpath(jobs, args)


    def _crawl2(self, args):
        """
        mode 2:
        search page url: domain+link+ (search keywords)"&cur_page="
        //
        in each news, href use relative link or full link
        <a href="/xwzx/roll/80285.htm" target="_blank" title="XXX">XXX</a>

        :param args: name, domain, url_list, page_url_string, xpath's etc...
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """

        name, domain, links = args["name"], args["domain"], args["link"]
        utils.log(f"Mode 1: Crawling {name}({domain}) with xpath (title | document); {dt.datetime.now()}")

        # Create url queues
        if "read_from_file" not in args:
            jobs = self.create_jobs(args)
        else:
            with open(args["read_from_file"], 'r') as f:
                jobs = f.read().split('\n')

        # Consume url queues
        return self.consume_jobs_all_text(jobs, args)

    def create_jobs(self, args):
        name, domain, links = args["name"], args["domain"], args["link"]
        page_url_prefix, page_url_suffix = args["page_url_prefix"], args["page_url_suffix"]

        document_urls = set()
        for link in links:
            # generate a list of index pages
            min_page, max_page = link["min_page"], link["max_page"]
            page_urls = [domain + link["url"] + page_url_prefix + str(page_i) + page_url_suffix \
                         for page_i in range(min_page, max_page + 1)][::-1]
            if min_page == 1:
                first_page = domain + link["url"] + page_url_prefix + '.html'
                page_urls.append(first_page)
                first_page_b = domain + link["url"] + '.html'
                page_urls.append(first_page_b)

            # Get a list of url pages (news posts) from an index page (search result)
            for page in page_urls:
                try:
                    urls = utils.get_urls_from_page(page, 0, args['url_xpath'])
                    time.sleep(1.4)
                except:
                    utils.log(f"Fail to retrieve urls from post list({page}).\n")
                    continue

                if not urls:
                    utils.log(f"Retrieved [0] urls from post list({page}).\n")
                    continue

                if args["href_pattern"] == "contain_domain":
                    pass
                elif args["href_pattern"] == "attached":
                    urls = [domain + u for u in urls]
                elif args["href_pattern"] == "relative":
                    prefix = re.match(r"http://[\w./]+/", domain + link["url"]).group(0)
                    urls = [prefix + u.split('/', 1)[1] for u in urls]

                utils.log(f"{len(urls)} urls parsed by xpath (in format: {urls[0]})\n")
                utils.log(f" .. from index page ({page}).\n")
                document_urls.update(urls)

        file_name = name.split(' ')[0] + '_job_urls.txt'
        utils.log(f"\n LOG: tasks ({len(document_urls)} urls) stored in {file_name}.\n")
        with open("data/" + file_name, 'w') as f:
            f.write("\n".join(document_urls))
        return list(document_urls)

    def consume_jobs_xpath(self, url_queue, args):
        utils.log(f"{len(url_queue)} urls ready to crawl.\n")
        name, domain, links = args["name"], args["domain"], args["link"]
        title_xpath, document_xpath = args['title_xpath'], args['document_xpath']

        urls_to_return = []
        count = 0
        for document_url in url_queue:
            count += 1
            utils.log(f"[{count}] Requesting: {document_url}\n")
            document_html = utils.get_html_from_url(document_url, args)
            if not document_html:
                utils.log(f"\n LOG: Fail to process: {domain + document_url}\n")
                continue


            root = etree.HTML(document_html)
            try:
                title = root.xpath(title_xpath)[0].text
            except Exception as et:
                utils.log(f"\n LOG: Exception: {et} while parsing Title {domain + document_url}.\n")
                title = ""

            try:
                document_text = root.xpath(document_xpath)
                document_string = " ".join([e.text for e in document_text if e.text])
            except Exception as ed:
                utils.log(f"\n LOG: Exception: {ed} while parsing Document {domain + document_url}.\n")
                continue

            found_log = "NOT "
            result, matched = self._contains_all_keywords(title + document_string)
            if result:
                urls_to_return.append(document_url)
                # urls_to_return.append(document_url)
                # self.urls_to_return.append(domain+document_url)
                found_log = ""
            utils.log(f"Processed: {document_string[:12]}(length: {len(document_string)})\n")
            utils.log(f".. in {title}({document_url}), keyword {found_log}found.{matched}\n")
            time.sleep(3)

            if count % 25 == 0:
                utils.log(f"LOG: Crawled: {count} pages(Current: {document_url})\n {dt.datetime.now()}\n")
                utils.log("\n".join(urls_to_return))

    def consume_jobs_all_text(self, url_queue, args):
        urls_to_return = []
        count = 0
        for document_url in url_queue:
            count += 1
            document_html = utils.get_html_from_url(document_url, args)

            plain_text = utils.remove_html_tags(document_html).strip()

            found_log = "NOT "
            result, matched = self._contains_all_keywords(plain_text)
            if result:
                urls_to_return.append(f"{document_url} | {matched}")
                found_log = ""
            utils.log(f"Processed: {plain_text[:12]}(length: {len(plain_text)}); keyword {found_log}found.{matched} \n")

            if count % 25 == 0:
                utils.log(f"LOG: Crawled: {count} pages(Current: {document_url})\n {dt.datetime.now()}\n")
                utils.log("\n".join(urls_to_return) + '\n')
            time.sleep(3)

        return urls_to_return

    def _contains_all_keywords(self, text):
        """
        :param text:
        :return: Boolean match result, List[String] matched keywords
        """
        def _contains_keyword(keyword_group):
            for keyword in self.keywords_json[keyword_group]:
                if keyword in text:
                    return True, keyword
            return False, ""

        keywords_matched = []
        for keyword_group in self.keywords_checklist:
            val, key = _contains_keyword(keyword_group)
            if not val:
                return False, []
            else:
                keywords_matched.append(key)
        return True, keywords_matched


if __name__ == '__main__':
    CHECKLIST = ["coal_keys", "countries", "action"]
    # CHECKLIST = ["coal_keys", "action"]  # test command
    spider = BeltRoadSpider(FILE_DIR + KEYWORDS_FILE, CHECKLIST, None)

    with open(FILE_DIR + WEBSITE_FILE, encoding="utf-8") as f:
        websites = json.load(f)

    urls = []
    for website in websites["crawling"]:
        if website["crawl"] == 0:
            continue

        params = website["data"]
        # Override with external job queue
        # params["read_from_file"] = "data/中国南方电网_job_urls.txt"
        # Override when read urls from file
        # params['href_pattern'] = "contain_domain"
        urls = spider.crawl(mode_int=params["crawl_mode_int"], args=params)

        with open("data/" + params["name"].split(" ")[0] + "output.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(urls))

    utils.log(f"LOG: Crawled {len(urls)} links that contains ALL keywords in {CHECKLIST}. ({dt.datetime.now()})\n")

