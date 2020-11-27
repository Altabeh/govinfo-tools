import hashlib
import json
import os
import re
import sys
from csv import writer
from datetime import datetime, timedelta
from glob import iglob
from multiprocessing import Pool, cpu_count
from pathlib import Path
from shutil import copy2, rmtree

import requests
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from utils import (backward_range_spit, f_date, ocr_converter,
                   pdftotext_converter)

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
    # Create appropriate json keys from relevant xml data stored in mods.xml from govinfo.
    tag_conversion = {'main': {'docclass': 'doc_class', 'category': 'category', 'collectioncode': 'collection',
                               'courttype': 'court_type', 'courtcode': 'court_code', 'courtcircuit': 'court_circuit', 'courtstate': 'court_state', 'casenumber': 'case_number', 'caseoffice': 'case_office', 'branch': 'branch', 'cause': 'cause', 'naturesuit': 'nature_of_suit', 'naturesuitcode': 'nature_of_suit_code', 'casetype': 'case_type', 'recordcreationdate': 'date_created', 'recordchangedate': 'date_changed', 'dateingested': 'date_ingested', 'languageterm': 'language_term', 'party': 'party'}, 'related': {'url': 'url', 'accessid': 'id', 'state': 'state', 'title': 'case_name', 'dockettext': 'docket_text', 'dateissued': 'date_issued', 'partnumber': 'part_number'}}

    def __init__(self, **kwargs):
        self.base_dir = kwargs.get("base_dir", BASE_DIR)
        self.today = datetime.date(datetime.now())
        self.final_date = kwargs.get("final_date", f_date(
            self.today))  # Final date to download data up to
        self.initial_date = kwargs.get(
            "initial_date", f_date(self.today - timedelta(days=1)))
        self.collection = kwargs.get("collection", 'USCOURTS')
        self.naturesuit = kwargs.get("naturesuit", 'Patent')
        self.page_size = kwargs.get("page_size", 100)
        if self.page_size not in self.__class__.page_size:
            self.page_size = 100
        self.page_offset = kwargs.get("page_offset", 0)
        if not isinstance(self.page_offset, int):
            self.page_offset = 0
        # A unique filename to label the data stored based on the search details
        self.hash_filename = kwargs.get('hash_filename', hashlib.md5(
            f'{self.collection}-{self.naturesuit}-{self.initial_date}-{self.final_date}'.encode('utf-8')).hexdigest())
        self.json_details_folder = os.path.join(
            os.path.join(self.base_dir, self.collection), self.naturesuit)
        os.makedirs(self.json_details_folder, exist_ok=True)
        # Json and text paths to files for which jsonify_metadata() failed to run
        self.failed_files = kwargs.get('failed_files', os.path.join(
            self.json_details_folder, 'failed_files'))
        os.makedirs(self.failed_files, exist_ok=True)

    def render_page(self, url):
        """
        Interactive selenium driver for active javascript execution that would
        be required in the websites that follow an ajax call for search functionaly.
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
        Compile the url for the results page given a date range and page.
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

        date_ranges = list(backward_range_spit(365, start=self.initial_date))
        pool = Pool(processes=cpu_count())
        for _ in tqdm(pool.imap(self.scrape_details, date_ranges, chunksize=100), total=len(date_ranges)):
            yield _

    def scrape_details(self, dates):
        """
        Scrape the details of links associated to each result.
        """

        start_date, end_date = dates
        r = self.render_page(self.compile_url(
            start_date, end_date, self.page_offset))
        page_seen = BS(str(r), 'html.parser')

        results_section = page_seen.find(id="recordCountId")
        record_number = '0'
        if results_section:
            record_number = results_section.replace(
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
        final and update dates.
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

        file_path = os.path.join(
            self.json_details_folder, f"{self.hash_filename}.json")
        with open(file_path, 'w') as output_file:
            json.dump(data, output_file, indent=4)
            print(
                f'---------| Results scraped from {self.initial_date} to {self.final_date} for the category "{self.naturesuit}" |----------')

        self.__class__.driver.quit()

    def prepare_metadata(self, json_details_path=None):
        """
        Prepare metadata by extracting id and case number for each
        case to compose appropriate urls for downloading later.
        """
        if json_details_path is None:
            json_details_path = os.path.join(
                self.json_details_folder, f'{self.hash_filename}.json')

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
        the metadata file mods.xml and pdf file for each case.
        """
        id_, num_ = params

        if id_:
            partial_id = id_.split('/')
            mods_xml = self.__class__.base_url + \
                f"metadata/granule/{id_}/mods.xml"
            pdf = self.__class__.base_url + \
                f"content/pkg/{partial_id[0]}/pdf/{partial_id[1]}.pdf"

            save_folder = os.path.join(
                os.path.join(self.json_details_folder, self.hash_filename), f'{partial_id[1].split("-")[1]}')
            os.makedirs(f'{save_folder}', exist_ok=True)

            metadata, pdf_data = requests.get(
                mods_xml).content, requests.get(pdf).content

            filename = partial_id[1].replace(f"{self.collection}-", "")
            with open(os.path.join(save_folder, f'{filename}.xml'), 'wb') as f1, open(os.path.join(save_folder, f'{filename}.pdf'), 'wb') as f2:
                f1.write(metadata)
                f2.write(pdf_data)
                print(
                    f'----------| The metadata and pdf for case number "{num_}" was downloaded successfully |----------')

    def collect_all_metadata(self, json_path=None, starting_index=0):
        all_composed_metadata = list(self.prepare_metadata(json_path))[
            starting_index:]
        pool = Pool(processes=cpu_count())
        for _ in tqdm(pool.imap(self.download_individual_metadata, all_composed_metadata, chunksize=100), total=len(all_composed_metadata)):
            pass

    def extract(self, *args):
        """
        Extract data from the content of mods.xml file and store
        it in a dictionary.

        Args:
        xml_tree ---> str: xml tree created by reading the mods.xml file
        data ---> dict: dictionary to store the extracted data
        tag ---> str: target tag name
        key ---> str: json key from the tag_conversion corresponding to tag 
        id_ ---> str: access id of the document
        doc_type ---> str: 'main' or 'related' if there is any sequential data
        """
        xml_elements, [xml_tree, data, tag, key, id_, doc_type] = "", args

        if doc_type == 'related':
            xml_elements = xml_tree.find(id=f"id-{self.collection}-{id_}")

        if doc_type == 'main':
            xml_elements = xml_tree

        tag_content = xml_elements.find_all(tag)

        data[key] = ""
        if len(tag_content) > 0:
            for inner_tag in tag_content:
                if re.search(r'displaylabel="PDF rendition"', str(inner_tag)):
                    data['pdf_url'] = inner_tag.get_text()

                elif re.search(r'displaylabel="Content Detail"', str(inner_tag)):
                    data['url'] = inner_tag.get_text()

                else:
                    if tag == 'party':
                        party_key = inner_tag.attrs['role'].lower().replace(
                            '-', ' ').replace(' ', '_')

                        party_value = data.get(party_key, [])
                        if not party_value:
                            data[party_key] = party_value

                        if inner_tag.attrs['fullname'] not in data[party_key]:
                            data[party_key].append(inner_tag.attrs['fullname'])
                    else:
                        data[key] = inner_tag.get_text()

        return data

    def exception(self, fields, file_path, filename):
        """
        Save file_path encountered an error into a csv file.
        """
        exc_type, value, traceback = sys.exc_info()
        assert exc_type.__name__ == 'NameError'
        if len(fields) <= 1:
            fields.append(exc_type.__name__)

        with open(os.path.join(self.failed_files, f'{self.hash_filename}.csv'), 'a+') as failed_files:
            file = failed_files.writer(file_path)
            file.writerow(fields)
        ext = os.path.splitext(self.paths_from_file)[1]
        print(
            f'Something went wrong with "{filename}{ext}" due to "{exc_type.__name__}"')

    def jsonify_metadata(self, xml_file):
        """
        Create json details from the xml and pdf files for each case.
        """

        with open(xml_file, 'r') as xml_content:
            filename = os.path.basename(os.path.splitext(xml_file)[0])
            xml_tree = BS(xml_content, 'lxml')
            data = {}

            try:
                for i in ['main', 'related']:
                    for tag, key in self.tag_conversion[i].items():
                        data = self.extract(
                            xml_tree, data, tag, key, filename, i)

            except Exception:
                fields = [xml_file]
                self.exception(fields, xml_file, filename)

        xml_dir = os.path.dirname(xml_file)
        json_path = os.path.join(xml_dir, 'json')
        os.makedirs(json_path, exist_ok=True)

        data['blocked'] = False
        data['plain_text'], error_output = self.extract_text(
            xml_file, filename)
        with open(os.path.join(json_path, f'{filename}.json'), 'w') as json_file:
            json.dump(data, json_file)
        print(
            f'---------| "{filename}" was created successfully |----------')

        if error_output:
            fields = [os.path.join(xml_dir, f'{filename}.pdf'), error_output]
            self.exception(fields, xml_file, filename)

    @staticmethod
    def extract_text(xml_file, filename):
        """
        Extract text from the pdf file associated to filename.
        """
        text_dir = os.path.join(os.path.dirname(xml_file), 'text')
        os.makedirs(text_dir, exist_ok=True)
        txt_file = os.path.join(text_dir, f'{filename}.txt')

        file_read, error = "", ""
        if not os.path.isfile(txt_file):
            error = pdftotext_converter(os.path.join(os.path.dirname(
                xml_file), f'{filename}.pdf'), text_dir)

        with open(os.path.join(text_dir, f'{filename}.txt'), 'r') as file:
            file_read = file.read()

        return file_read, error

    def bulk_jsonify(self):
        """
        Jsonify the files generated by jsonify_metadata in bulk.
        """
        all_xml_files = list(
            iglob(os.path.join(self.json_details_folder, f'**/{self.hash_filename}/*.xml')))
        pool = Pool(processes=cpu_count())
        tqdm(pool.imap_unordered(self.jsonify_metadata,
                                 all_xml_files, chunksize=100), total=len(all_xml_files))
        pool.close()
        pool.join()

    def delete_folder(self, folders=[]):
        """
        Delete folders.
        """
        if folders:
            for folder in folders:
                all_subfolders = iglob(os.path.join(
                    self.json_details_folder, f'**/{folder}'))
                for dir_ in all_subfolders:
                    rmtree(dir_, ignore_errors=False, onerror=None)
                    print(
                        f'---------| "{dir_}" was successfully deleted. |----------')
        else:
            print('No folder was found to be deleted.')

    def move_files(self, extensions=[]):
        """
        Can be used to move the files with given extensions
        from subdirectories of the details folder into the 
        hashed subfolder.
        """
        if extensions:
            for ext in extensions:
                all_file = iglob(os.path.join(
                    self.json_details_folder, f'**/*.{ext}'))
                for file in all_file:
                    target_dir = f'{Path(file).parent}/{self.hash_filename}/'
                    os.makedirs(target_dir, exist_ok=True)
                    copy2(file, target_dir)
                    os.remove(file)
                    print(
                        f'---------| "{file}" was successfully moved to {target_dir}. |----------')
        else:
            print('No file with given extensions was detected.')


#if __name__ == "__main__":
#    gd = GovDownload()
#    gd.bulk_jsonify()
