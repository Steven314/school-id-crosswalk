import os
import requests
import zipfile
import time

def conditional_download(url: str, dest: str, sleep: bool = False) -> None:
    """Conditionally download a file from a URL.

    Args:
        url (str): The URL.
        dest (str): The destination file.
        sleep (bool, optional): Option to include a one second wait after
            downloading. Defaults to False.
    """

    if not os.path.exists(dest):
        resp = requests.get(url)

        with open(dest, mode="wb") as file:
            file.write(resp.content)

        if sleep:
            time.sleep(1)

    return None


def conditional_extract(raw_file: str, dest: str) -> None:
    """Conditionally extract the contents of the ZIP file.

    Args:
        raw_file (str): The location of the ZIP file.
        dest (str): THe directory where to put the extraction.
    """

    if not os.path.exists(raw_file):
        raise FileNotFoundError("The ZIP file was not found.")

    if not os.path.exists(dest):
        with zipfile.ZipFile(raw_file, "r") as z:
            z.extractall(dest)

    return None
