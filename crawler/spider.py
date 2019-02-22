import json
import request
import re

KEYWORDS_FILE = "crawler/static/keywords.json"
WEBSITE_FILE = "crawler/static/websites.json"


class BeltRoadSpider:
    def __init__(self, bri_country_list, action_words, start_date):
        self.bri_country_list = bri_country_list
        self.action_words = action_words
        self.start_date = start_date
        self.urls_to_return = []

    def crawl(self, domain, mode, args):
        """

        :param domain: domain, typically url of a search page or list page under news channel
        :param mode: 'BY_PAGE or 'BY_DATE'
        :param args: if mode == 'BY_PAGE': max. page corresponding to the starting page
        :return: a list of urls that meet the search criteria, using bri_country_list and action_words
        """

    def _get_urls_from_page(self, page, mode):
        """

        :param search_page:
        :param mode:
        :return: List[string], a list of urls
        """


    def _get_post_date_from_document(self, document, mode):
        """
        :param document:
        :return: Time stamp in string YY-MM-DD
        """
        pass

    def _contains_BRI_country(self, text):
        """

        :param text:
        :return: Boolean:
        """

if __name__ == '__main__':
    pass