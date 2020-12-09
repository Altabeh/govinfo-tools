from datetime import datetime, timedelta
from pathlib import Path
from subprocess import PIPE, CalledProcessError, Popen, check_output
from tempfile import TemporaryDirectory

import pytesseract
from pdf2image import convert_from_path

__all__ = ['rm_tree', 'p_date', 'f_date', 'date_range_pars', 'forward_range_spit',
           'backward_range_spit', 'pdf_to_text', 'ocr_to_text', 'get_page_count']


def rm_tree(path):
    """
    Remove file/directory under path.
    """
    path = Path(path)
    for child in path.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    path.rmdir()


def p_date(string):
    """
    Returns a datetime object from a string of the format '%Y-%m-%d'.
    """
    return datetime.strptime(string, '%Y-%m-%d')


def f_date(date):
    """
    Convert a date object into '%Y-%m-%d' string.
    """
    return date.strftime('%Y-%m-%d')


def date_range_pars(range_days, start, end):
    """
    Chops the dates from a start date till some later date,
    into date objects separated by a given number of days.

    Args
    ----
    :param range_days: ---> int: number of days.
    :param start: ---> str: start date.
    :param end: ---> str: end date.   
    """
    start = p_date(start)
    end = p_date(end)
    range_ = timedelta(days=range_days)
    return start, end, range_


def forward_range_spit(range_days, start, end=None):
    """
    Generate tuples with intervals within a given range of dates (forward).
    forward_date_range(10, '2020-10-01', '2020-10-30')

    1st yield = ('2012-01-01', '2012-01-03')
    2nd yield = ('2012-01-04', '2012-01-05')
    """
    if end is None:
        end = f_date(datetime.date(datetime.now()))

    start, end, range_ = date_range_pars(range_days, start, end)
    stop = end - range_

    while start < stop:
        current = start + range_
        yield f_date(start), f_date(current)
        start = current + timedelta(days=1)

    yield f_date(start), f_date(end)


def backward_range_spit(range_days, start, end=None):
    """
    Generate tuples with intervals from given range of dates (backward)
    backward_date_range(10, '2020-10-01', '2020-10-30')

    1st yield = ('2020-10-01', '2020-10-11')
    2nd yield = ('2020-10-12', '2020-10-22')
    """

    if end is None:
        end = f_date(datetime.date(datetime.now()))

    start, end, range_ = date_range_pars(range_days, start, end)
    stop = start + range_

    while end > stop:
        current = end - range_
        yield f_date(current), f_date(end)
        end = current - timedelta(days=1)

    yield f_date(start), f_date(end)


def pdf_to_text(pdf_path, target_dir):
    """
    Convert pdf at `pdf_path` to a txt file in `target_dir` using xpdf.
    """
    file_name = Path(pdf_path).stem
    command = ["pdftotext", "-layout", pdf_path,
               str(Path(target_dir) / f'{file_name}.txt')]
    proc = Popen(
        command, stdout=PIPE, stderr=PIPE)
    proc.wait()
    (stdout, stderr) = proc.communicate()
    if proc.returncode:
        return stderr
    return ''


def ocr_to_text(pdf_path, resolution=200):
    """
    Convert ocr to text using pytesseract and imagemagick.

    Args
    ----
    :param pdf_path: ---> str: the path to a pdf document.
    :param resolution: ---> int: resolution of the converted images.
    """
    page_count = get_page_count(pdf_path)
    page_text = []
    for page in range(1, page_count + 1, 10):
        with TemporaryDirectory() as path:
            images = convert_from_path(
                pdf_path, output_folder=path, fmt='jpeg', dpi=resolution, first_page=page, last_page=min(page + 9, page_count))
            for img in images:
                text = pytesseract.image_to_string(img, lang='eng')
                page_text.append(text)
    return page_text


def get_page_count(pdf_path):
    """
    Use xpdf's pdfinfo to extract the number of pages in a pdf file.
    """
    try:
        output = check_output(["pdfinfo", pdf_path]).decode()
        pages_line = [line for line in output.splitlines()
                      if "Pages:" in line][0]
        num_pages = int(pages_line.split(":")[1])
        return num_pages

    except CalledProcessError:
        return 0
