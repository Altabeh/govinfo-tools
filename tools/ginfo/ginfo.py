
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
from csv import QUOTE_NONE, writer
from datetime import datetime
from functools import partial
from glob import glob, iglob
from multiprocessing import Pool
from pathlib import Path

import requests
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from ginfo.utils import (backward_range_spit, f_date, get_page_count,
                         ocrtotext_converter, p_date, pdftotext_converter, rm_tree)

__author__ = {"github.com/": ["altabeh"]}
__all__ = ['Ginfo']


class Ginfo(object):
    """
    A class for limitless searching, crawling, downloading, organizing,
    extracting, serializing and saving (meta)data from www.govinfo.gov.
    """
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver = webdriver.Chrome(options=options)
    base_url = 'https://www.govinfo.gov/'
    page_size = [10, 50, 100]
    data = {}
    # Create appropriate json keys from relevant Descriptive Metadata (mods) stored in mods.xml from govinfo.
    tag_conversion = {'main': {'docclass': 'doc_class', 'category': 'category', 'collectioncode': 'collection',
                               'courttype': 'court_type', 'courtcode': 'court_code', 'courtcircuit': 'court_circuit', 'courtstate': 'court_state', 'casenumber': 'case_number', 'caseoffice': 'case_office', 'branch': 'branch', 'cause': 'cause', 'naturesuit': 'nature_of_suit', 'naturesuitcode': 'nature_of_suit_code', 'casetype': 'case_type', 'recordcreationdate': 'date_created', 'recordchangedate': 'date_changed', 'dateingested': 'date_ingested', 'languageterm': 'language_term', 'party': 'party', 'identifier': 'preferred_citation'}, 'related': {'url': 'url', 'accessid': 'id', 'state': 'state', 'title': 'case_name', 'dockettext': 'docket_text', 'dateissued': 'date_issued', 'partnumber': 'part_number'}}

    def __init__(self, **kwargs):
        # Set the default base directory to the parent of current repo.
        self.base_dir = kwargs.get('base_dir', Path(
            '__file__').resolve().parents[5].__str__())
        self.today = datetime.date(datetime.now())

        # Final date to download data up to.
        self.final_date = kwargs.get('final_date', f_date(
            self.today))

        # Initial date from which data is downloaded. Defaults to '1990-01-01'.
        self.initial_date = kwargs.get(
            'initial_date', '1990-01-01')

        # Collection name. Defaults to 'USCOURTS'.
        # Visit https://www.govinfo.gov/help/whats-available
        self.collection = kwargs.get('collection', 'USCOURTS')

        # Nature of suit. Defaults to 'Patent'.
        self.nature_suit = kwargs.get('nature_suit', 'Patent')

        # Number of results on to be shown on each page (can be either 10, 50 or 100). Defaults to 100.
        self.page_size = kwargs.get('page_size', 100)
        if self.page_size not in self.__class__.page_size:
            self.page_size = 100

        # The result page under consideration. Defaults to 0.
        self.page_offset = kwargs.get('page_offset', 0)
        if not isinstance(self.page_offset, int):
            self.page_offset = 0

        # A unique filename to label the data stored based on the search details.
        self.hash_filename = kwargs.get('hash_filename', hashlib.md5(
            f'{self.collection}-{self.nature_suit}-{self.initial_date}-{self.final_date}'.encode('utf-8')).hexdigest())

        # The parent folder where all the details are saved for each collection and nature of suit.
        self.json_details_folder = Path(
            self.base_dir) / self.collection / self.nature_suit
        self.json_details_folder.mkdir(parents=True, exist_ok=True)

        # Json and text paths to files for which serialize_metadata method failed to run.
        self.failed_files = kwargs.get(
            'failed_files', str(self.json_details_folder / 'failed_files'))
        Path(self.failed_files).mkdir(parents=True, exist_ok=True)

        # Control the ocr_conversion of pdf files.
        self.ocr_conversion = kwargs.get('ocr_conversion', True)

        # Print details for the workflow in all the methods.
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

        Args:
            start_date ---> str: starting date from which results will be shown
                            on govinfo.gov.
            end_date ---> str: date beyond which results will not be shown
                            on govinfo.gov search page.
            page ---> int: current page.
        """
        url = f'{self.__class__.base_url}app/search/%7B"query"%3A"collection%3A({self.collection})%20AND%20publishdate%3Arange({start_date}%2C{end_date})%20AND%20naturesuit%3A({self.nature_suit})"%2C"offset"%3A{page}%2C"pageSize"%3A"{self.page_size}"%7D'
        return url

    @staticmethod
    def find_link(page_seen):
        """
        Find links to the results and collect their attributes addthis:title and addthis:url.

        Args:
            page_seen ---> BeautifulSoup class: object receiving the stringified 
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

    def search_results(self):
        """
        Search for entries on the results page whose details are to be scraped.
        """
        # Split dates into intervals of 365 days. If the difference is less than a year,
        # it will automatically fall back to the remaining days.
        date_ranges = list(backward_range_spit(
            365, self.initial_date, self.final_date))
        with Pool() as p:
            for _ in tqdm(p.imap_unordered(self.scrape_details, date_ranges), total=len(date_ranges)):
                yield _

    def scrape_details(self, dates):
        """
        Scrape the details of links associated to each result.

        Args:
            dates ---> tuple: range of dates on which scraping results
                       will be carried out.
        """
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
        Scrape results and extract case details and save everything
        in a json file and seal it with initial, final and update dates.
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

        file_path = self.json_details_folder / \
            f'{self.hash_filename}.json'
        with open(file_path, 'w') as output_file:
            json.dump(data, output_file, indent=4)
            if self.print_to_console:
                print(
                    f'Results scraped from {self.initial_date} to {self.final_date} for the category "{self.nature_suit}"')
        self.__class__.driver.quit()

    def prepare_details(self, json_details_path=None):
        """
        Prepare details by extracting id and case number for each
        case saved into a file under `json_details_path`
        to compose appropriate urls for downloading later.
        """
        if json_details_path is None:
            json_details_path = self.json_details_folder / \
                f'{self.hash_filename}.json'
        try:
            with open(json_details_path, 'r') as output_file:
                loaded_data = json.load(output_file)
                for key in loaded_data.keys():
                    if isinstance(fn := loaded_data[key], list):
                        for elem in fn:
                            case_id = elem['url'].replace(
                                '/app/details/', '')
                            yield case_id
        except FileNotFoundError:
            raise Exception(
                f'{json_details_path.__str__()} is not a file or directory.')

    def download_details(self, case_id):
        """
        Take json file generated by seal_results at json_details_path
        and download metadata file mods.xml and pdf file for each case.

        Args:
            case_id ---> str: Package ID/Granule ID.

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
                url = ''
                if file_ext == 'xml':
                    url = self.__class__.base_url + \
                        f'metadata/granule/{case_id}/mods.{file_ext}'
                if file_ext == 'pdf':
                    url = self.__class__.base_url + \
                        f'content/pkg/{package_id}/{file_ext}/{granule_id}.{file_ext}'
                save_folder = self.json_details_folder / \
                    granule_id.split('-')[1] / self.hash_filename / file_ext
                save_folder.mkdir(parents=True, exist_ok=True)
                path = save_folder / f'{filename}.{file_ext}'
                if not path.is_file():
                    data = requests.get(url).content
                    path.write_bytes(data)
            if self.print_to_console:
                print(
                    f'The metadata and pdf for case number {filename} was downloaded successfully')

    def collect_data_metadata(self, json_details_path=None):
        """
        Prepare and collect all the data and metadata files
        whose urls are saved at a json file under json_details_path.

        Args:
            json_details_path ---> str: path to a json file where metadata urls are
                                        stored.

        Example json file in which urls of xml and pdf files are stored:
                 "~/USCOURTS/Patent/07abf09ca4d5661daca0b42c573b77ae.json"
        """
        # Create a list that is composed of each case details in the form of package id/granule id.
        # E.g. [USCOURTS-mad-1_18-cv-10568/USCOURTS-mad-1_18-cv-10568-1, ...]
        composed_details = list(self.prepare_details(json_details_path))
        with Pool() as p:
            for _ in tqdm(p.imap_unordered(self.download_details, composed_details), total=len(composed_details)):
                pass

    def extract_metadata(self, *args):
        """
        Extract data from the content of mods.xml file and store
        it in a dictionary.

        Args:
            xml_tree ---> str: xml tree created by reading the mods.xml file.
            data ---> dict: dictionary to store the extracted data.
            tag ---> str: target tag name.
            key ---> str: json key from the `tag_conversion` corresponding to `tag`.
            id_ ---> str: access id of the document.
            doc_type ---> str: `'main'` or `'related'` if there is any sequential data.
        """
        xml_elements, [xml_tree, data, tag, key, id_, doc_type] = '', args
        if doc_type == 'related':
            xml_elements = xml_tree.find(id=f'id-{self.collection}-{id_}')
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

    def exception(self, error_root, filename):
        """
        Save the objects wrapped in `error_root` into a csv file.

        Args:
            error_root ---> list of size 2: 1st element is the path to a file;
                            2nd element is the exception name.
            filename ---> str: name of the xml file.
        """
        if len(error_root) <= 1:
            exc_type, value, traceback = sys.exc_info()
            assert exc_type.__name__ == 'NameError'
            error_root.append(exc_type.__name__)

        with open(Path(self.failed_files) / f'error-log.csv', 'a+', newline='') as failed_files:
            csvfile = writer(failed_files, delimiter='\t',
                             quoting=QUOTE_NONE, quotechar='',  lineterminator='\n')
            csvfile.writerow(error_root)

        # File extension can be extracted from the file path.
        ext = Path(error_root[0]).stem
        if self.print_to_console:
            print(
                f'Something went wrong with {filename}.{ext} due to "{error_root[1]}"')

    def serialize_metadata(self, xml_path):
        """
        Create details serialized into a json from the xml file
        at xml_path and the text of pdf file for each case.
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
                error_root = [xml_path, e]
                self.exception(error_root, filename)
        parent_dir = Path(xml_path).parents[1]
        json_path = parent_dir / 'json'
        json_path.mkdir(parents=True, exist_ok=True)
        data['blocked'] = False
        text, error_output = self.extract_text(
            xml_path, filename)
        pdf_path = str(parent_dir / 'pdf' / f'{filename}.pdf')
        data['page_count'] = get_page_count(pdf_path)
        if error_output:
            error_root = [pdf_path, error_output]
            self.exception(error_root, filename)

        # Check to see if the pdf is ocr or not.
        plain_text, data['ocr'], citation = self.check_ocr(
            text, data['court_type'], data['preferred_citation'])

        # If pdf is ocr, begin processing the pdf again.
        if data['ocr'] and self.ocr_conversion:
            try:
                ocr_text = '\n\n'.join(list(ocrtotext_converter(pdf_path)))
                plain_text = self.header_remove(ocr_text, citation)
            except Exception as e:
                if self.print_to_console:
                    print(
                        f'Encountered "{e}" while extracting text from the ocr file {filename}.pdf')
        data['plain_text'] = plain_text
        with open(json_path / f'{filename}.json', 'w') as json_file:
            json.dump(data, json_file)
            if not self.ocr_conversion and data['ocr']:
                if self.print_to_console:
                    print(
                        f'{filename}.pdf needs ocr conversion; {filename}.json was created successfully')
            else:
                if self.print_to_console:
                    print(
                        f'{filename}.json was created successfully')

    def extract_text(self, xml_path, filename):
        """
        Extract text from the pdf file associated to filename.

        Args: 
            xml_path ---> str: path to the metadata file.
            filename ---> str: the name of metadata file.
        """
        parent_dir = Path(xml_path).parents[1]
        text_dir = parent_dir / 'text'
        text_dir.mkdir(parents=True, exist_ok=True)
        txt_path = text_dir / f'{filename}.txt'
        pdf_path = parent_dir / 'pdf' / f'{filename}.pdf'

        file_read, error = '', ''
        if not pdf_path.is_file():
            return file_read, error
        if not txt_path.is_file():
            try:
                error = pdftotext_converter(pdf_path, text_dir)
            except Exception as e:
                error_root = [pdf_path.__str__(), e]
                self.exception(error_root, filename)
                pass
        if txt_path.is_file():
            file_read = txt_path.read_text()

        # Remove the txt file.
        txt_path.unlink()
        return file_read, error

    def bulk_serialize(self, xml_paths=None):
        """
        Serialize the files generated by serialize_metadata method in bulk.

        Args:
             xml_paths ---> list: external list of metadata (xml) files.
        """
        if not xml_paths:
            xml_paths = glob(
                str(self.json_details_folder / f'**/{self.hash_filename}/xml/*.xml'))
        with Pool() as p:
            for _ in tqdm(p.imap_unordered(self.serialize_metadata, xml_paths), total=len(xml_paths)):
                pass

    @staticmethod
    def header_remove(string, citation):
        """
        Pattern to match and remove the header used by govinfo.gov
        to sign every document in their database using a `citation`.

        Example match: Case 4:17-cv-00237-RLY-DML Document 70 Filed 03/01/19 Page 1 of 12 PageID #:
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

        Example:
               `preferred_citation`: "1:06-cv-00007;06-007`.
               `court_type`: `District`.
               `citation`: `1:06-cv-00007`.
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

    def delete_folder(self, folders=[], top_level_subdirectory='**'):
        """
        Delete folders. 

        Args: 
            folders ---> list: list of folder names to be targeted.
            top_level_subdirectory ---> str: the immediate subdirectory under 
                                        `json_details_folder` and above folders.

        Example:  ~/USCOURTS/Patent/**/json ---> e.g. ~/USCOURTS/Patent/ca11/json
        """
        if folders:
            for folder in folders:
                all_subfolders = iglob(str(self.json_details_folder /
                                           f'{top_level_subdirectory}/{folder}'))
                for dir_ in all_subfolders:
                    rm_tree(dir_)
                    if self.print_to_console:
                        print(f'{dir_} was successfully deleted')
        else:
            if self.print_to_console:
                print('No folder was found to be deleted.')

    def move_files(self, extensions=[], top_level_subdirectory='**', target_dir=None):
        """
        Can be used to move the files with given extensions
        from subdirectories of the details folder into the 
        hashed subfolder.

        Args: 
            extensions ---> list: list of extensions of files to be moved.
            top_level_subdirectory ---> str: the immediate subdirectory under 
                                        `json_details_folder` and above folders.
            target_dir ---> str: target directory to move files into.
        """
        if extensions:
            for ext in extensions:
                all_file = iglob(
                    str(self.json_details_folder / f'{top_level_subdirectory}/*.{ext}'))
                for f in all_file:
                    fp = Path(f)
                    if target_dir is None:
                        target_dir = fp.parent / ext
                    else:
                        target_dir = Path(target_dir)
                    target_dir.mkdir(parents=True, exist_ok=True)
                    fp.rename(target_dir / fp.name)
                    if self.print_to_console:
                        print(f'{f} was successfully moved to "{target_dir}"')
        else:
            if self.print_to_console:
                print('No file with given extensions was detected')

    def check_failed_files(self):
        """
        Checks to find possibly a list of metadata filenames that have 
        failed to be serialized into json files during serialization.
        """
        all_data = {}
        for ext in ['json', 'xml']:
            partial_paths = f'**/**/{ext}/*.{ext}'
            paths = iglob(str(self.json_details_folder / partial_paths))
            all_data[ext] = set([Path(data).stem for data in paths])

        # List of failed filenames.
        failed_filenames = list(all_data['xml'] - all_data['json'])

        return failed_filenames, all_data

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
        failed_filenames, all_data = self.check_failed_files()

        # Attempt to run serialization if a processor encountered a syntax error somewhere.
        if failed_filenames:
            if self.print_to_console:
                print(
                    f'The number of failed files: {len(failed_filenames)} -- Attempting one more time to run serialization...')
            failed_files = [glob(str(
                self.json_details_folder / f'**/**/{e}.xml'))[0] for e in failed_filenames]
            self.bulk_serialize(failed_files)
            failed_filenames, all_data = self.check_failed_files()

        # Create records item that contains a list of all available files.
        for case_id in sorted(all_data['xml']):
            case_key = f'{self.collection}-{case_id}'
            case_abbr = case_id.split('-')[0]

            d = info['records'].get(case_abbr, {'number_of_records': 0})
            if not d.get(case_key, None):
                key_info = {'has_json': False}
                if case_id in all_data['json']:
                    key_info['has_json'] = True
                    info['total_json_files'] += 1

                d['number_of_records'] += 1
                d[case_key] = key_info
                info['records'][case_abbr] = d
                info['total_cases'] += 1

        # Add the range of dates covering the filing dates of cases packaged into info.json.
        dc = info['dates_covered']
        all_downloaded_data = iglob(str(self.json_details_folder / '*.json'))
        for path in all_downloaded_data:
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
        if self.print_to_console:
            print(
                f'info.json was created at {info["time_created"]} successfully')

    def gzip_court_data(self, court_related, gzip_folder):
        """
        Create individual court bulk data files under each court folder 
        inside json_details_folder.

        Args:
            court_related ---> a pathlib obj: any folder/file containing information about all/individual
                               court data.
            gzip_folder ---> a pathlib obj: folder hosting the gzipped data.
        """
        with tarfile.open(str(gzip_folder / f'{court_related.stem}.tar.gz'), 'w:gz') as tar:
            # Keep the archive structure intact with relative_to.
            tar.add(court_related, arcname=court_related.relative_to(
                court_related.parent))
        if self.print_to_console:
            print(
                f'{str(court_related)}.tar.gz was created at {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} successfully')

    def gzip_bulk_data(self):
        """
        Create a gzip version of the bulk data containing gzipped data of all courts.
        """
        # Get all court directories/related-files; exclude "failed_files" folder and hidden items.
        court_related = [path for path in self.json_details_folder.glob(
            '*/') if not str(path).endswith('_files') and not str(path.stem).startswith('.')]

        # Create a folder which will host the gzipped data.
        gzip_folder = Path(self.base_dir) / self.collection / 'gzip'
        gzip_folder.mkdir(parents=True, exist_ok=True)
        with Pool() as p:
            iterable = [(ct, gzip_folder) for ct in court_related]
            for _ in tqdm(p.imap_unordered(partial(self.gzip_court_data, gzip_folder=gzip_folder), court_related), total=len(iterable)):
                pass
        if self.print_to_console:
            print(f'Creating the gzipped version of the whole data now...')

        # Save the bulk data file.
        bulk_filename = f'{self.nature_suit}.tar.gz'
        bulk_data_path = str(gzip_folder.parent / bulk_filename)
        with tarfile.open(bulk_data_path, 'w:gz') as tar:
            for item in gzip_folder.glob('*'):
                tar.add(item, arcname=item.relative_to(item.parent))

        # Delete the gzip folder.
        rm_tree(gzip_folder)
        if self.print_to_console:
            print(
                f'Bulk data file {self.nature_suit}.tar.gz was created at {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} successfully')
        return bulk_data_path
