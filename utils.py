import io
from datetime import datetime, timedelta
from subprocess import PIPE, CalledProcessError, Popen, check_output

import pytesseract
from PIL import Image
from wand.image import Image as wi
from pathlib import Path


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
    
    Args: 
        range_days ---> int: number of days.
        start ---> str: start date.
        end ---> str: end date.   
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


def pdftotext_converter(pdf_path, target_dir):
    """Convert pdf at pdf_file to a txt file in target_dir using xpdf."""
    file_name = Path(pdf_path).stem
    command = ["pdftotext", "-layout", pdf_path, str(Path(target_dir) / f'{file_name}.txt')]
    proc = Popen(
        command, stdout=PIPE, stderr=PIPE)
    proc.wait()
    (stdout, stderr) = proc.communicate()
    if proc.returncode:
        return stderr
    return ''


def ocrtotext_converter(pdf_path):
    """
    Convert ocr to text using pytesseract and imagemagick.
    """
    pdfFile = wi(filename=pdf_path, resolution=300)
    image = pdfFile.convert('jpeg')

    imageBlobs = [wi(image=img).make_blob('jpeg') for img in image.sequence]

    for imgBlob in imageBlobs:
        image = Image.open(io.BytesIO(imgBlob))
        text = pytesseract.image_to_string(image, lang='eng')
        yield text


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
