import concurrent.futures as future
from datetime import datetime, timedelta
from os import cpu_count
from pathlib import Path
from subprocess import PIPE, CalledProcessError, Popen, check_output
from tempfile import TemporaryDirectory

from cv2 import cv2
from pdf2image import convert_from_path

from ginfo.tesseract import Tesseract

__all__ = [
    "rm_tree",
    "p_date",
    "f_date",
    "date_range_pars",
    "forward_range_spit",
    "get_tesseract_text",
    "backward_range_spit",
    "pdf_to_text",
    "ocr_to_text",
    "get_page_count",
]

TESS = Tesseract()


def rm_tree(path):
    """
    Remove file/directory under path.
    """
    path = Path(path)
    for child in path.glob("*"):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    path.rmdir()


def p_date(string):
    """
    Returns a datetime object from a string of the format '%Y-%m-%d'.
    """
    return datetime.strptime(string, "%Y-%m-%d")


def f_date(date):
    """
    Convert a date object into '%Y-%m-%d' string.
    """
    return date.strftime("%Y-%m-%d")


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
    command = [
        "pdftotext",
        "-layout",
        pdf_path,
        str(Path(target_dir) / f"{file_name}.txt"),
    ]
    proc = Popen(command, stdout=PIPE, stderr=PIPE)
    proc.wait()
    (stdout, stderr) = proc.communicate()
    if proc.returncode:
        return stderr
    return ""


def get_tesseract_text(img_path, **kwargs):
    """
    Use tesseract api to get the text from the images directly.

    Keywords
    --------
    A dictionary of key, val that tesseract api can accept.
    """
    imcv = cv2.imread(img_path)
    height, width, depth = imcv.shape
    for key, val in kwargs.items():
        TESS.set_variable(key, val)
    TESS.set_image(imcv.ctypes, width, height, depth)
    gettext = TESS.get_text()
    return gettext


def wrap_get_tesseract_text(img_path, kwargs):
    """
    A wrapper for `get_tesseract_text` to be used in multiprocessing/concurrency.
    """
    return get_tesseract_text(img_path, **kwargs)


def ocr_to_text(pdf_path, batch_size=10, **kwargs):
    """
    Convert ocr to text using path2image, cv2 and tesseract api.
    `kwargs` belong to the function `get_tesseract_text`.

    Args
    ----
    :param pdf_path: ---> str: the path to a pdf document.
    :param batch_size: ---> int: size of batches of converted pages
                                 fed into `get_tesseract_text`.
    """
    resolution = kwargs.get("user_defined_dpi", "250")
    page_count = get_page_count(pdf_path)
    cpus = cpu_count()
    # To use up all cpus
    if cpus > batch_size:
        batch_size = cpus
    iter_ = 0
    for page in range(1, page_count + 1, batch_size):
        with TemporaryDirectory() as path:
            path_to_pages = convert_from_path(
                pdf_path,
                output_folder=path,
                fmt="tiff",
                dpi=int(resolution),
                first_page=page,
                last_page=min(page + batch_size - 1, page_count),
                paths_only=True,
            )

            with future.ProcessPoolExecutor(max_workers=cpus) as executor:
                tasks = {
                    executor.submit(wrap_get_tesseract_text, page, kwargs): i
                    + 1
                    + iter_ * batch_size
                    for i, page in enumerate(path_to_pages)
                }
                for f in future.as_completed(tasks):
                    page_number = tasks[f]
                    try:
                        data = f.result(), page_number
                        yield data
                    except Exception as e:
                        print(f"page #{page_number} generated an exception: {e}")
        iter_ += 1


def get_page_count(pdf_path):
    """
    Use xpdf's pdfinfo to extract the number of pages in a pdf file.
    """
    try:
        output = check_output(["pdfinfo", pdf_path]).decode()
        pages_line = [line for line in output.splitlines() if "Pages:" in line][0]
        num_pages = int(pages_line.split(":")[1])
        return num_pages

    except (CalledProcessError, UnicodeDecodeError):
        return 0
