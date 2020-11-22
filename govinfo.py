import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

import requests
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from utils import _strftime, backward_date_range

# Avoids "RecursionError: maximum recursion depth exceeded in comparison."
sys.setrecursionlimit(150000000)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath('__file__')))


__author__ = "Alireza Behtash"
__copyright__ = "Copyright 2020, Stackslaw.com"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Alireza Behtash"
__email__ = "proof.beh@gmail.com"


class GovDownload(object):
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    driver = webdriver.Chrome(options=options)
    base_url = "https://www.govinfo.gov/"
    page_size = [10, 50, 100]
    data = {}

    def __init__(self, **kwargs):
        self.today = datetime.date(datetime.now())
        self.final_date = kwargs.get("final_date", _strftime(self.today))
        self.initial_date = kwargs.get(
            "initial_date", _strftime(self.today - timedelta(days=1)))
        self.collection = kwargs.get("collection", 'USCOURTS')
        self.naturesuit = kwargs.get("naturesuit", 'Patent')
        self.page_size = kwargs.get("page_size", 100)
        if self.page_size not in self.__class__.page_size:
            self.page_size = 100
        self.page_offset = kwargs.get("page_offset", 0)
        if not isinstance(self.page_offset, int):
            self.page_offset = 0
        self.hash_file = hashlib.md5(
            f'{self.collection}-{self.naturesuit}-{self.initial_date}-{self.final_date}'.encode('utf-8')).hexdigest()
        self.json_details_folder = os.path.join(
            os.path.join(BASE_DIR, self.collection), self.naturesuit)
        os.makedirs(self.json_details_folder, exist_ok=True)

    def render_page(self, url):
        """
        Interactive selenium driver for active javascript execution that would
        be required in the websites that follow an ajax call for search functionaly
        """
        self.__class__.driver.get(url)

        try:
            WebDriverWait(self.__class__.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, "btn-group-horizontal"))
            )
        finally:
            r = self.__class__.driver.page_source
            return r

    def compile_url(self, start_date, end_date, page):
        """
        Compiles the url for the results page given a date range and page
        """
        url = f'{self.__class__.base_url}app/search/%7B"query"%3A"collection%3A({self.collection})%20AND%20publishdate%3Arange({start_date}%2C{end_date})%20AND%20naturesuit%3A({self.naturesuit})"%2C"offset"%3A{page}%2C"pageSize"%3A"{self.page_size}"%7D'
        return url

    @staticmethod
    def find_link(page_seen):
        """
        Find links to the results and collect their attributes addthis:title and addthis:url
        """
        share_info = page_seen.find_all('a', attrs={'class': 'displayShare'})

        for info in share_info:
            fn = BS(str(info), 'html.parser')
            dig_name_num = re.findall(
                r'^(.*?) - (.*)', fn.find('a').attrs['addthis:title'])[0]
            link_attrs = {"num": dig_name_num[0], 'name': dig_name_num[1], "url": BS(
                str(info), 'html.parser').find('a').attrs['addthis:url']}

            yield link_attrs

    def search_results(self):
        """
        Search for entries on the results page whose details are to be scraped
        """

        date_ranges = list(backward_date_range(365, start=self.initial_date))
        pool = Pool(processes=cpu_count())
        for _ in tqdm(pool.imap(self.scrape_details, date_ranges, chunksize=100), total=len(date_ranges)):
            yield _

    def scrape_details(self, dates):
        """
        Scrape the details of links associated to each result
        """

        start_date, end_date = dates[0], dates[1]
        r = self.render_page(self.compile_url(
            start_date, end_date, self.page_offset))
        page_seen = BS(str(r), 'html.parser')
        record_number = page_seen.find(id="recordCountId").get_text().replace(
            ' Records', '').replace(',', '')

        max_page = 0
        next_page_element = page_seen.find('li', class_="next")

        last_page = 'Previous'

        if next_page_element:
            last_page = next_page_element.find_previous_sibling(
                'li').find('a').get_text()

        if last_page != 'Previous':
            if record_number:
                if int(record_number) <= 10000:
                    max_page = int(last_page)
                else:
                    max_page = 10000 / int(self.page_size)

        self.__class__.data[f'{start_date}-to-{end_date}_{self.page_offset+1}'] = list(
            self.find_link(page_seen))

        if max_page > 0:
            for page in range(1, max_page):
                r = self.render_page(self.compile_url(
                    start_date, end_date, page))
                page_seen = BS(str(r), 'html.parser')
                self.__class__.data[f'{start_date}_to_{end_date}_{page+1}'] = list(
                    self.find_link(page_seen))

        return self.__class__.data

    def seal_results(self):
        """
        Scrape results and extract patent case details and
        save everything in a json file and seal it with initial,
        final and update dates
        """
        data = {}
        number_of_keys = 0
        for item in self.search_results():
            for key in item.keys():
                data[key] = item[key]
                if isinstance(fn := item[key], list):
                    number_of_keys += len(fn)

        data['initial_date'] = self.initial_date
        data['final_date'] = self.final_date
        data['update_date'] = str(self.today)
        data['total_cases'] = number_of_keys

        file_path = os.path.join(self.json_details_folder, f"{self.hash_file}.json")

        with open(file_path, 'w') as output_file:
            json.dump(data, output_file, indent=4)
            print(
                f'---------| Results scraped from {self.initial_date} to {self.final_date} for the category "{self.naturesuit}" |----------')

        self.__class__.driver.quit()

    def prepare_metadata(self, json_details_path=None):
        """
        Prepare metadata by extracting id and case number for each
        case to compose appropriate urls for downloading later 
        """
        if json_details_path is None:
            json_details_path = os.path.join(
                self.json_details_folder, f'{self.hash_file}.json')

        try:
            with open(json_details_path, 'r') as output_file:
                loaded_data = json.load(output_file)

                for key in loaded_data.keys():
                    if isinstance(fn := loaded_data[key], list):
                        for elem in fn:
                            id_, num_ = elem['url'].replace(
                                "/app/details/", ''), elem['num']
                            yield id_, num_

        except FileNotFoundError:
            raise Exception(
                f'{json_details_path} is not a file or directory.')

    def download_individual_metadata(self, params):
        """
        Take json file generated by seal_result at json_details_path and download
        the metadata file mods.xml and pdf file for each case
        """
        id_, num_ = params[0], params[1]

        partial_id = id_.split('/')
        mods_xml = self.__class__.base_url + f"metadata/granule/{id_}/mods.xml"
        pdf = self.__class__.base_url + \
            f"content/pkg/{partial_id[0]}/pdf/{partial_id[1]}.pdf"

        save_folder = os.path.join(self.json_details_folder, f'{partial_id[1].split("-")[1]}')
        os.makedirs(save_folder, exist_ok=True)

        metadata, pdf_data = requests.get(
            mods_xml).content, requests.get(pdf).content
        
        filename = partial_id[1].replace(f"{self.collection}-", "")
        with open(os.path.join(save_folder, f'{filename}.xml'), 'wb') as f1, open(os.path.join(save_folder, f'{filename}.pdf'), 'wb') as f2:
            f1.write(metadata)
            f2.write(pdf_data)
            print(
                f'---------| The metadata and pdf for case number "{num_}" was downloaded successfully |----------')

    def collect_all_metadata(self, json_path=None, starting_index=0):
        all_composed_metadata = list(self.prepare_metadata(json_path))[starting_index:]
        pool = Pool(processes=cpu_count())
        for _ in tqdm(pool.imap(self.download_individual_metadata, all_composed_metadata, chunksize=100), total=len(all_composed_metadata)):
            pass

    