import os
import requests
import zipfile
import time


def census_download(url: str, dest_file: str) -> None:
    """Conditionally Download Raw Data From a URL

    Args:
        url (str): The URL where the data is downloaded.
        dest_file (str): The file where the data is saved. This is inside the `raw-data` directory.
    """

    path = os.path.join("raw-data", dest_file)

    if not os.path.exists(path):
        resp = requests.get(url)

        with open(path, mode="wb") as file:
            file.write(resp.content)

        # sleep to avoid being rate limited
        time.sleep(1)

    return None


def census_extract(file: str, dest_dir: str):
    if not os.path.exists(file):
        raise FileNotFoundError("The ZIP file was not found.")

    if not os.path.exists(dest_dir):
        with zipfile.ZipFile(file, "r") as z:
            z.extractall(dest_dir)
