
"""
This module provides a topic-wise search-and-download crawler
called `ginfo` for any court opinion document available under
advanced search feature of https://www.govinfo.gov.

You can choose court opinions with a nature of suit and
the resulting search will only yield
orders/opinions within the chosen scope.
"""
from __future__ import absolute_import

import hashlib
import json
import re
import sys
import tarfile
from concurrent.futures import ProcessPoolExecutor as future_pool
from csv import QUOTE_NONE, reader, writer
from datetime import datetime
from functools import partial
from glob import glob, iglob
from multiprocessing import Pool, cpu_count
from pathlib import Path

import requests
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm
from ginfo.utils import (backward_range_spit, f_date, get_page_count,
                         ocr_to_text, p_date, pdf_to_text, rm_tree)

__author__ = {"github.com/": ["altabeh"]}
__all__ = ['Ginfo']


class Ginfo(object):
    """
    A class for limitless searching, crawling, downloading, organizing,
    extracting, serializing and saving (meta)data from www.govinfo.gov.
    """
    today = datetime.date(datetime.now())
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver = webdriver.Chrome(options=options)
    base_url = 'https://www.govinfo.gov/'
    page_size = [10, 50, 100]
    # Create appropriate json keys from relevant Descriptive Metadata (mods) stored in mods.xml from govinfo.
    tag_conversion = {
        'main': {'docclass': 'doc_class', 'category': 'category', 'collectioncode': 'collection',
                 'courttype': 'court_type', 'courtcode': 'court_code', 'courtcircuit': 'court_circuit',
                 'courtstate': 'court_state', 'casenumber': 'case_number', 'caseoffice': 'case_office',
                               'branch': 'branch', 'cause': 'cause', 'naturesuit': 'nature_of_suit',
                               'naturesuitcode': 'nature_of_suit_code', 'casetype': 'case_type',
                               'recordcreationdate': 'date_created', 'recordchangedate': 'date_changed',
                               'dateingested': 'date_ingested', 'languageterm': 'language_term',
                               'party': 'party', 'identifier': 'preferred_citation'},
        'related': {'url': 'url', 'accessid': 'id', 'state': 'state', 'title': 'case_name',
                    'dockettext': 'docket_text', 'dateissued': 'date_issued', 'partnumber': 'part_number'}
    }
    download_errors = {}

    def __init__(self, **kwargs):
        """
        Args
        ____
        :param base_dir: ---> str: set the directory to the parent of current repo.
        :param processes: ---> int: number of logical processes used in multiprocessing.
        :param initial_date: ---> str: initial date from which data is downloaded.
                          Defaults to `1990-01-01`.
        :param final_date: ---> str: final date to download data up to. Defaults to today.
        :param collection: ---> str: collection name. Defaults to `USCOURTS`.
                        Visit https://www.govinfo.gov/help/whats-available
        :param nature_suit: ---> str: nature of suit. Defaults to `Patent`.
        :param page_size: ---> int: number of results on to be shown on each page 
                       (can be either `10`, `50` or `100`). Defaults to `100`.
        :param page_offset: ---> int: the result page under consideration. Defaults to `0`.
        :param hash_filename: ---> str: a unique filename to label the data stored based
                           on the search details.
        :param json_details_folder: ---> pathlib obj: the parent folder where all the details 
                                  are saved for each collection and nature of suit.
        :param errors: ---> pathlib obj: path to the `errors` directory.
        :param ocr_conversion: ---> bool: control the ocr conversion of pdf files.
        :param ocr_config: ---> dict: sets the tesseract-ocr configuration.
        :param print_to_console: ---> bool: print details for the workflow in all the methods.
        """
        self.base_dir = kwargs.get('base_dir', Path(
            '__file__').resolve().parents[5].__str__())
        self.processes = kwargs.get('processes', cpu_count())
        self.initial_date = kwargs.get(
            'initial_date', '1990-01-01')
        self.final_date = kwargs.get('final_date', f_date(
            self.__class__.today))
        self.collection = kwargs.get('collection', 'USCOURTS')
        self.nature_suit = kwargs.get('nature_suit', 'Patent')
        self.page_size = kwargs.get('page_size', 100)
        if self.page_size not in self.__class__.page_size:
            self.page_size = 100
        self.page_offset = kwargs.get('page_offset', 0)
        if not isinstance(self.page_offset, int):
            self.page_offset = 0
        self.hash_filename = kwargs.get('hash_filename', hashlib.md5(
            f'{self.collection}-{self.nature_suit}-{self.initial_date}-{self.final_date}'.encode('utf-8')).hexdigest())
        self.json_details_folder = self._create(kwargs.get('json_details_folder', Path(
            self.base_dir) / self.collection / self.nature_suit))
        self.errors = self._create(kwargs.get(
            'errors', Path(self.json_details_folder) / 'errors'))
        self.ocr_conversion = kwargs.get('ocr_conversion', True)
        # The following default tesseract configuration works best with court opinions.
        self.ocr_config = kwargs.get('ocr_config') or {
            'grayscale': 'true', 'user_defined_dpi': '250', 'oem': '1'}
        self.print_to_console = kwargs.get('print_to_console', False)

    def render_page(self, url):
        """
        Interactive selenium driver for active javascript execution that would
        be required in the websites that follow an ajax call for search functionality.
        """
        self.__class__.driver.get(url)
        try:
            WebDriverWait(self.__class__.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'btn-group-horizontal'))
            )
        finally:
            r = self.__class__.driver.page_source
            return r

    def compile_url(self, start_date, end_date, page):
        """
        Compile the url for the results page given a date range and page.

        Args
        ----
        :param start_date: ---> str: starting date from which results will be shown
                                    on govinfo.gov.
        :param end_date: ---> str: date beyond which results will not be shown
                                  on govinfo.gov search page.
        :param page: ---> int: current page as seen in the pagination div.
        """
        url = f'{self.__class__.base_url}app/search/%7B"query"%3A"collection%3A({self.collection})%20AND%20publishdate%3Arange({start_date}%2C{end_date})%20AND%20naturesuit%3A({self.nature_suit})"%2C"offset"%3A{page}%2C"pageSize"%3A"{self.page_size}"%7D'
        return url

    @staticmethod
    def find_link(page_seen):
        """
        Find links to the results and collect their attributes
        `addthis:title` and `addthis:url`.

        Args
        ----
        :param page_seen: ---> BeautifulSoup class: object receiving the stringified 
                               html/xml page.
        """
        share_info = page_seen.find_all('a', attrs={'class': 'displayShare'})
        for info in share_info:
            fn = BS(str(info), 'html.parser')
            dig_name_num = re.findall(
                r'^(.*?) - (.*)', fn.find('a').attrs['addthis:title'])[0]
            link_attrs = {'num': dig_name_num[0], 'name': dig_name_num[1], 'url': BS(
                str(info), 'html.parser').find('a').attrs['addthis:url']}
            yield link_attrs

    def search_results(self, processes=None):
        """
        Search for entries on the results page whose details are to be scraped.
        """
        # Split dates in intervals of 365 days. If the difference is less than a year,
        # it will automatically fall back to the remaining days.
        if processes is None:
            processes = self.processes
        date_ranges = list(backward_range_spit(
            365, self.initial_date, self.final_date))
        with Pool(processes=processes) as p:
            for _ in tqdm(p.imap_unordered(self.scrape_details, date_ranges), total=len(date_ranges)):
                yield _

    def scrape_details(self, dates):
        """
        Scrape the details of links associated to each result.

        Args
        ----
        :param dates: ---> tuple: range of dates on which scraping results
                           will be carried out.
        """
        data = {}
        start_date, end_date = dates
        r = self.render_page(self.compile_url(
            start_date, end_date, self.page_offset))
        page_seen = BS(str(r), 'html.parser')
        results_section = page_seen.find(id='recordCountId')
        record_number = '0'
        
        if results_section:
            record_number = results_section.get_text().replace(
                ' Records', '').replace(',', '')
    
        max_page = 0
        next_page_element = page_seen.find('li', class_='next')
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
    
        data[f'{start_date}-to-{end_date}_{self.page_offset+1}'] = list(
            self.find_link(page_seen))
        
        if max_page > 0:
            for page in range(1, max_page):
                r = self.render_page(self.compile_url(
                    start_date, end_date, page))
                page_seen = BS(str(r), 'html.parser')
                data[f'{start_date}_to_{end_date}_{page+1}'] = list(
                    self.find_link(page_seen))
        return data

    def seal_results(self):
        """
        Scrape results and extract case details and save everything
        in a json file and seal it with initial, final and update dates.
        """
        data = {}
        number_of_keys = 0
        for item in self.search_results():
            for key, value in item.items():
                data[key] = value
                if isinstance(value, list):
                    number_of_keys += len(value)
        
        data['initial_date'] = self.initial_date
        data['final_date'] = self.final_date
        data['update_date'] = str(self.__class__.today)
        data['total_cases'] = number_of_keys

        file_path = self.json_details_folder / \
            f'{self.hash_filename}.json'
        with open(file_path, 'w') as output_file:
            json.dump(data, output_file, indent=4)
            self._print(self.seal_results)
        self.__class__.driver.quit()

    def extract_case_id(self, json_details_path=None):
        """
        Extract case id from each url saved in the file under `json_details_path`
        to compose appropriate urls for downloading details for each case on govinfo.gov.
        """
        if json_details_path is None:
            json_details_path = self.json_details_folder / \
                f'{self.hash_filename}.json'
        try:
            with open(json_details_path, 'r') as output_file:
                loaded_data = json.load(output_file)
                for value in loaded_data.values():
                    if isinstance(value, list):
                        for elem in value:
                            case_id = elem['url'].replace(
                                '/app/details/', '')
                            yield case_id
        except FileNotFoundError:
            raise Exception(
                f'{json_details_path.__str__()} is not a file or directory.')

    def download_details(self, case_id, failed=False):
        """
        Take json file generated by `seal_results` at `json_details_path`
        and download metadata file mods.xml and pdf file for each case.

        Args
        ----
        :param case_id: ---> str: Package ID/Granule ID.
        :param failed: ---> bool: retry downloading a failed response.

        Example:
                case_id: USCOURTS-mad-1_18-cv-10568/USCOURTS-`mad-1_18-cv-10568`-`1`
                where `mad-1_18-cv-10568` is the case number; `1` is the Sequence Number;
                `USCOURTS-mad-1_18-cv-10568` is the Package ID and `USCOURTS-mad-1_18-cv-10568-1`
                is the Granuale ID.
        """
        if case_id:
            # package_id = Package ID & granule_id = Package ID as described in
            # https://www.govinfo.gov/help/uscourts
            [package_id, granule_id] = case_id.split('/')

            # Create a unique filename that will be used to save both xml and pdf files.
            # filename = {court_code}-{case_number}-{sequence_number}
            filename = granule_id.replace(f'{self.collection}-', '')
            for file_ext in ['xml', 'pdf']:
                error_ = {}
                url = ''
                if file_ext == 'xml':
                    url = self.__class__.base_url + \
                        f'metadata/granule/{case_id}/mods.{file_ext}'
                if file_ext == 'pdf':
                    url = self.__class__.base_url + \
                        f'content/pkg/{package_id}/{file_ext}/{granule_id}.{file_ext}'
                save_folder = self._create(self.json_details_folder /
                                           granule_id.split('-')[1] / self.hash_filename / file_ext)
                path = save_folder / f'{filename}.{file_ext}'
                try:
                    if not path.is_file():
                        response = requests.get(url, timeout=None)
                        status = response.status_code
                        if status == 200:
                            path.write_bytes(response.content)
                            if failed:
                                self.__class__.download_errors.pop(
                                    case_id, None)
                        else:
                            error_ = {case_id: status}
                except Exception as e:
                    error_ = {case_id: e}
                if error_:
                    self.__class__.download_errors = {
                        **self.__class__.download_errors, **error_}
                else:
                    self._print(self.download_details, filename)

    def parallel_download(self, json_details_path=None, processes=None):
        """
        Parallelize applying `download_details` method to the list of case IDs
        created using the json file under `json_details_path` and `extract_case_id`
        method. 

        Args
        ----
        :param json_details_path: ---> str: path to a json file where metadata urls are
                                  stored.

        Example
        -------
        json file in which urls of xml and pdf files are stored:
        "~/USCOURTS/Patent/07abf09ca4d5661daca0b42c573b77ae.json"
        """
        # Create a list that is composed of each case details in the form of package id/granule id.
        # E.g. [USCOURTS-mad-1_18-cv-10568/USCOURTS-mad-1_18-cv-10568-1, ...]
        composed_details = list(self.extract_case_id(json_details_path))
        if processes is None:
            processes = self.processes
        with Pool(processes=processes) as p:
            for _ in tqdm(
                    p.imap_unordered(partial(self.download_details, failed=False), composed_details), total=len(composed_details)):
                pass
            failed = self.__class__.download_errors
            if failed:
                self._print(self.parallel_download)
                download_errors = failed.keys()
                for _ in tqdm(
                        p.imap_unordered(partial(self.download_details, failed=True), download_errors), total=len(download_errors)):
                    pass
                failed = self.__class__.download_errors
                self._logger(
                    map(list, zip(failed.keys(), failed.values())), filename='download-log')
                # Garbage collect download_errors:
                self.__class__.download_errors = None

    def extract_metadata(self, *args):
        """
        Extract data from the content of mods.xml file and store
        it in a dictionary.

        Args
        ----
        :param xml_tree ---> str: xml tree created by reading the mods.xml file.
        :param data: ---> dict: dictionary to store the extracted data.
        :param tag: ---> str: target tag name.
        :param key: ---> str: json key from the `tag_conversion` corresponding to `tag`.
        :param access_id: ---> str: access id of the document.
        :param doc_type: ---> str: `main`, or `related` if there is any sequential data.
        """
        xml_elements, [xml_tree, data, tag,
                       key, access_id, doc_type] = '', args

        if doc_type == 'related':
            xml_elements = xml_tree.find(
                id=f'id-{self.collection}-{access_id}')
    
        if doc_type == 'main':
            xml_elements = xml_tree
        
        if tag != 'identifier':
            tag_content = xml_elements.find_all(tag)

        else:
            # Only pick up identifier with role="preferred citation".
            tag_content = xml_elements.find_all(
                tag, attrs={'type': 'preferred citation'})

        data[key] = ''
        parties = {}
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
                        party_value = parties.get(party_key, [])
                        if not party_value:
                            parties[party_key] = party_value
                        if inner_tag.attrs['fullname'] not in parties[party_key]:
                            parties[party_key].append(
                                inner_tag.attrs['fullname'])
                        data['party'] = parties
                    else:
                        data[key] = inner_tag.get_text()
        return data

    def serialize_metadata(self, xml_path):
        """
        Create details for each case by serializing relevant xml data at `xml_path`
        into a json file and updating the json data with the text of pdf file
        using the key 'plain_text'.
        """
        with open(xml_path, 'r') as xml_content:
            filename = Path(xml_path).stem
            xml_tree = BS(xml_content, 'lxml')
            data = {}
            try:
                for i in ['main', 'related']:
                    for tag, key in self.tag_conversion[i].items():
                        data = self.extract_metadata(
                            xml_tree, data, tag, key, filename, i)
            except Exception as e:
                self._exception([xml_path, e], filename)
    
        parent_dir = Path(xml_path).parents[1]
        json_parent = self._create(parent_dir / 'json')
        data['blocked'] = False
        text, error_output = self.extract_text(
            xml_path, filename)
        pdf_path = str(parent_dir / 'pdf' / f'{filename}.pdf')
        page_count = data['page_count'] = get_page_count(pdf_path)
        if error_output:
            self._exception([pdf_path, error_output], filename)
        
        # Check if the pdf is ocr or not.
        plain_text, data['ocr'], citation = self.check_ocr(
            text, data['court_type'], data['preferred_citation'])

        # If pdf is ocr, begin processing the pdf again.
        if data['ocr'] and self.ocr_conversion:
            try:
                numpage_text_bundle = sorted(
                    [page for page in ocr_to_text(pdf_path, **self.ocr_config)], key=lambda x: x[1])
                ocr_text = '\n'.join([page[0] for page in numpage_text_bundle])
                plain_text = self.header_remove(ocr_text, citation)
            except Exception as e:
                self._exception([xml_path, e], filename)
                self._print(self.serialize_metadata, e, filename, order=1)

        data['plain_text'] = plain_text
        json_path = json_parent / f'{filename}.json'
        with open(json_path, 'w') as json_file:
            json.dump(data, json_file)
            if not self.ocr_conversion and data['ocr']:
                self._print(self.serialize_metadata, *[filename]*2, order=2)
            else:
                self._print(self.serialize_metadata, filename, order=3)

        csv_row = [json_path.__str__()]
        # Something went wrong with pdfinfo & poppler:
        if not page_count:
            csv_row.append('Not processed')
        
        # Something went wrong with ocr or pdftotext:
        elif not len(plain_text):
            csv_row.append('PDF is problematic')
        
        if len(csv_row) > 1:
            self._logger(csv_row, mode='a+', filename='process-log')

    def extract_text(self, xml_path, filename):
        """
        Extract text from the pdf file associated to filename.

        Args
        ---- 
        :param xml_path: ---> str: path to the metadata file.
        :param filename: ---> str: the name of metadata file.
        """
        parent_dir = Path(xml_path).parents[1]
        text_dir = self._create(parent_dir / 'text')
        txt_path = text_dir / f'{filename}.txt'
        pdf_path = parent_dir / 'pdf' / f'{filename}.pdf'
        file_read, error = '', ''
        if not pdf_path.is_file():
            return file_read, error
        if not txt_path.is_file():
            try:
                error = pdf_to_text(pdf_path, text_dir)
            except Exception as e:
                self._exception([pdf_path.__str__(), e], filename)
        if txt_path.is_file():
            file_read = txt_path.read_text()
            txt_path.unlink()
        return file_read, error

    def bulk_serialize(self, xml_paths=None):
        """
        Serialize the files generated by `serialize_metadata` method in bulk.

        Args
        ----
        :param xml_paths: ---> list: external list of metadata (xml) files.
        """
        if not xml_paths:
            xml_paths = glob(
                str(self.json_details_folder / f'**/{self.hash_filename}/xml/*.xml'))
        with future_pool(max_workers=self.processes) as p:
            for _ in tqdm(p.map(self.serialize_metadata, xml_paths), total=len(xml_paths)):
                pass

    @staticmethod
    def header_remove(string, citation):
        """
        Pattern to match and remove the header used by govinfo.gov
        to sign every document in their database using a `citation`.

        Example
        -------
        Case 4:17-cv-00237-RLY-DML Document 70 Filed 03/01/19 Page 1 of 12 PageID #:
                                               <pageID>
        where `citation` is `4:17-cv-00237`.
        """
        regex = r'.*?(?=' + citation + \
            r').*?(?=\d{1,2}/\d{1,2}/\d{2,4}).*(?:\n.*)?(?:(?=<?[Pp]a?ge?).*)'
        string = re.sub(regex, '', string)
        return string

    def check_ocr(self, text, court_type, preferred_citation):
        """
        Rules defined at https://www.govinfo.gov/help/uscourts 
        for obtaining the correct citation from the preferred_citation
        based on court_type.

        Example
        -------
        preferred_citation: `1:06-cv-00007;06-007`.
        court_type: `District`.
        citation: `1:06-cv-00007`.
        """
        citation = ''
        if court_type in ['Appellate', 'Bankruptcy']:
            citation = preferred_citation.split(';')[-1]
        
        if court_type == 'District':
            citation = preferred_citation.split(';')[0]
    
        text = self.header_remove(text, citation)
        # Get the first remaining 60 words to see if ocr document is encountered.
        words = [w for w in re.sub(r'\W+', ' ', text).split(' ')[:60] if w]
    
        # If the number of leftover words is more than 50, do not activate ocr converter.
        if len(words) > 50:
            return text, False, citation
        return '', True, citation

    def check_failed_files(self):
        """
        Check to find possibly a list of metadata filenames that have 
        failed to be serialized into json files during serialization.
        """
        data = {}
        for ext in ['json', 'xml']:
            partial_paths = f'**/**/{ext}/*.{ext}'
            paths = iglob(str(self.json_details_folder / partial_paths))
            data[ext] = set([Path(data).stem for data in paths])
        # List of failed filenames.
        failed_filenames = list(data['xml'] - data['json'])
        return failed_filenames, data

    def seal_bulk_data(self):
        """
        Creates a json file info.json that entails information 
        about how many pdf files, metadata files (xml and json)
        and txt to seal the bulk data.
        """
        # Points to a file that keeps track of updates.
        info_path = self.json_details_folder / 'info.json'
        info = {}
        info['dates_covered'], info['total_json_files'], info['total_cases'], info['records'] = [
        ], 0, 0, {}
        if info_path.is_file():
            with open(info_path, 'r') as f:
                info = json.load(f)
        failed_filenames, data = self.check_failed_files()

        # Attempt to run serialization if a processor encountered a syntax error somewhere.
        if failed_filenames:
            self._print(self.seal_bulk_data, len(failed_filenames), order=1)
            failed_files = [glob(str(
                self.json_details_folder / f'**/**/{e}.xml'))[0] for e in failed_filenames]
            self.bulk_serialize(failed_files)
            failed_filenames, data = self.check_failed_files()

        # Create records item that contains a list of all available files.
        for case_id in sorted(data['xml']):
            case_key = f'{self.collection}-{case_id}'
            case_abbr = case_id.split('-')[0]
            d = info['records'].get(case_abbr, {'number_of_records': 0})
            if not d.get(case_key, None):
                key_info = {'has_json': False}
                if case_id in data['json']:
                    key_info['has_json'] = True
                    info['total_json_files'] += 1
                d['number_of_records'] += 1
                d[case_key] = key_info
                info['records'][case_abbr] = d
                info['total_cases'] += 1
    
        # Add the range of dates covering the filing dates of cases packaged in info.json.
        dc = info['dates_covered']
        downloaded_data = iglob(str(self.json_details_folder / '*.json'))
        for path in downloaded_data:
            with open(path, 'r') as json_file:
                d_data = json.load(json_file)
                try:
                    date_range = [d_data['initial_date'], d_data['final_date']]
                    if date_range not in dc:
                        dc.append(date_range)
                except KeyError:
                    pass

        info['dates_covered'] = sorted(dc, key=lambda x: p_date(x[0]))
        info['collection'] = self.collection
        info['nature_of_suit'] = self.nature_suit
        info['time_created'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

        with open(info_path, 'w') as output_file:
            json.dump(info, output_file, indent=4)

        self._print(self.seal_bulk_data, info["time_created"], order=2)

    def gzip_court_data(self, court_related, gzip_folder):
        """
        Create individual court bulk data files under each court folder 
        inside json_details_folder.

        Args
        ----
        :param court_related: ---> a pathlib obj: any folder/file containing information about all/individual
                              court data.
        :param gzip_folder: ---> a pathlib obj: folder hosting the gzipped data.
        """
        with tarfile.open(str(gzip_folder / f'{court_related.stem}.tar.gz'), 'w:gz') as tar:
            # Keep the archive structure intact with relative_to.
            tar.add(court_related, arcname=court_related.relative_to(
                court_related.parent))
            self._print(self.gzip_court_data, str(court_related))

    def gzip_bulk_data(self):
        """
        Create a gzip version of the bulk data containing gzipped data of all courts.
        """
        # Get all court directories/related-files; exclude "errors" folder and hidden items.
        court_related = [path for path in self.json_details_folder.glob(
            '*/') if str(path) != 'errors' and not str(path.stem).startswith('.')]
    
        # Create a folder which will host the gzipped data.
        gzip_folder = self._create(
            Path(self.base_dir) / self.collection / 'gzip')
        with future_pool(max_workers=self.processes) as p:
            iterable = [(ct, gzip_folder) for ct in court_related]
            for _ in tqdm(p.map(
                    partial(self.gzip_court_data, gzip_folder=gzip_folder), court_related),
                    total=len(iterable)):
                pass
        self._print(self.gzip_bulk_data, order=1)
    
        # Save the bulk data file.
        bulk_filename = f'{self.nature_suit}.tar.gz'
        bulk_data_path = str(gzip_folder.parent / bulk_filename)
        with tarfile.open(bulk_data_path, 'w:gz') as tar:
            for item in gzip_folder.glob('*'):
                tar.add(item, arcname=item.relative_to(item.parent))
    
        # Delete the gzip folder.
        rm_tree(gzip_folder)
        self._print(self.gzip_bulk_data, order=2)
        return bulk_data_path

    def _logger(self, csv_row=[], mode='w+', filename=None, read_only=False):
        """
        Write errors to a csv file for later tracking across the class.

        Args
        ----
        :param csv_row: --> list: the row(s) to be written to the error log file.
        :param read_only: --> bool: read the log file only if it exists and return
                                    paths to the logged files because of an error.
        """
        if filename is None:
            filename = 'exception-log'
            mode = 'a+'
        path = self.errors / f'{filename}.csv'
        if read_only:
            if path.is_file():
                with open(path, 'r', newline='') as file:
                    rows = reader(file, delimiter='\t')
                    if rows:
                        return [row[0] for row in rows]
            return []

        if csv_row:
            with open(path, mode, newline='') as f:
                csvfile = writer(f, delimiter='\t', quoting=QUOTE_NONE,
                                 quotechar='',  lineterminator='\n')
                # If csv_row contains nested lists:
                if isinstance(csv_row[0], list):
                    csvfile.writerows(csv_row)
                else:
                    csvfile.writerow(csv_row)

    def _exception(self, error_root, filename):
        """
        Save the objects wrapped in `error_root` as part of
        exceptions encountered across the class in a csv file.

        Args
        ----
        :param error_root: ---> list of size 2: 1st element is the path to a file;
                                2nd element is the exception name.
        :param filename: ---> str: name of the metadata file.
        """
        traceback = ''
        if len(error_root) <= 1:
            exc_type, value, traceback = sys.exc_info()
            assert exc_type.__name__ == 'NameError'
            error_root.extend([exc_type.__name__, traceback])
            self._logger(error_root)

        # File extension can be extracted from the file path.
        self._print(self._exception, filename, Path(
            error_root[0]).suffix, error_root[1], traceback)

    def _delete(self, folders=[], top_level_subdirectory='**'):
        """
        Delete folders. 

        Args
        ---- 
        :param folders: ---> list: list of folder names to be targeted.
        :param top_level_subdirectory: ---> str: the immediate subdirectory under 
                                           `json_details_folder` and above folders.

        Example
        -------
        ~/USCOURTS/Patent/**/json ---> e.g. ~/USCOURTS/Patent/ca11/json
        """
        if folders:
            for folder in folders:
                subfolders = iglob(str(self.json_details_folder /
                                           f'{top_level_subdirectory}/{folder}'))
                for d in subfolders:
                    rm_tree(d)
                    self._print(self._delete, d, order=1)
        else:
            self._print(self._delete, order=2)

    def _move(self, extensions=[], top_level_subdirectory='**', target_dir=None):
        """
        Can be used to move the files with given extensions
        from subdirectories of the details folder into the 
        hashed subfolder.

        Args
        ----
        :param extensions: ---> list: list of extensions of files to be moved.
        :param top_level_subdirectory: ---> str: the immediate subdirectory under 
                                            `json_details_folder` and above folders.
        :param target_dir: ---> str: target directory to move files into.
        """
        if extensions:
            for ext in extensions:
                files = iglob(
                    str(self.json_details_folder / f'{top_level_subdirectory}/*.{ext}'))
                for f in files:
                    fp = Path(f)
                    if target_dir is None:
                        target_dir = fp.parent / ext
                    else:
                        target_dir = Path(target_dir)
                    fp.rename(self._create(target_dir) / fp.name)
                    self._print(self._move, f, str(target_dir), order=1)
        else:
            self._print(self._move, order=2)

    @staticmethod
    def _create(path):
        """
        Create a directory under `path`.
        """
        if isinstance(path, str):
            path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _print(self, func, *args, **kwargs):
        """
        Allow the print statements in a function `func` to go into effect.
        """
        if self.print_to_console:
            # `Order` determines the order of the print statement if more than
            # one statement exists in the same function. 
            order = kwargs.get('order', None)
            if self.print_to_console:
                if func.__name__ == 'seal_results':
                    print(
                        f'Results scraped from {self.initial_date} to {self.final_date} for the category "{self.nature_suit}"')

                if func.__name__ == 'download_details':
                    print(
                        f'The metadata and pdf for case number {args[0]} was downloaded successfully')

                if func.__name__ == 'parallel_download':
                    print(
                        f'Proceeding to retry downloading files failed in first attempt...')

                if func.__name__ == 'serialize_metadata':
                    if order and order == 1:
                        print(
                            f'Encountered "{args[0]}" while extracting text from the ocr file {args[1]}.pdf')
                    if order and order == 2:
                        print(
                            f'{args[0]}.pdf needs ocr conversion; {args[1]}.json was created successfully')
                    else:
                        print(
                            f'{args[0]}.json was created successfully')

                if func.__name__ == 'seal_bulk_data':
                    if order and order == 1:
                        print(
                            f'The number of failed files: {args[0]} -- Attempting one more time to run serialization...')
                    else:
                        print(
                            f'info.json was created at {args[0]} successfully')

                if func.__name__ == 'gzip_court_data':
                    print(
                        f'{args[0]}.tar.gz was created at {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} successfully')

                if func.__name__ == 'gzip_bulk_data':
                    if order and order == 1:
                        print(
                            f'Creating the gzipped version of the whole data now...')
                    else:
                        print(
                            f'Bulk data file {self.nature_suit}.tar.gz was created at {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} successfully')

                if func.__name__ == '_exception':
                    print(
                        f'Something went wrong with {args[0]}{args[1]} due to "{args[2]} {args[3]}"')

                if func.__name__ == '_delete':
                    if order and order == 1:
                        print(f'{args[0]} was successfully deleted')
                    else:
                        print('No folder was found to be deleted.')

                if func.__name__ == '_move':
                    if order and order == 1:
                        print(
                            f'{args[0]} was successfully moved to "{args[1]}"')
                    else:
                        print('No file with given extensions was detected')
