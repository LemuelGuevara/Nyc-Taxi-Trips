import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import requests
import yaml
from alive_progress import alive_bar as ab
from azure.storage.blob import ContainerClient
from bs4 import BeautifulSoup as bs
from tqdm import tqdm as tq

from azure_storage.azure_storage import AzureContainer

DOMAIN = 'https://www.nyc.gov'
URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
START_DATE = 2009
END_DATE = 2022
FILE_TYPE = '.parquet'
DIR_PATH = 'data/'

class BeautifulSoup:

    def _get_soup(self, url: str) -> bs:
        """Gets the soup from the website"""

        return bs(requests.get(url).text, 'html.parser')


class Scraper(BeautifulSoup):

    file_size = 0

    """ def scrape_file_size(self, files: list) -> int:
        Scrapes the file size from the website

        print('Getting file size...')
        for scraped_link in tq(files):
            res = requests.head(scraped_link)
            self.file_size += int(res.headers['content-length'])
        print(f'File size: {self.file_size} bytes')

        # Asks if the user wants to continue downloading
        continue_dowload = input('Do you want to continue? (y/n) ')
        if continue_dowload == 'y':
            return self.file_size
        else:
            print('Ok, bye!')
            exit() """

    def scrape_file_links(self, url: str) -> list:
        """Scrapes the file links from the website"""

        links = []
        print('Getting file links...')
        for link in self._get_soup(url).find_all('a'):
            for date in range(START_DATE, END_DATE):
                scraped_link = link.get('href')
                if str(date) in scraped_link and FILE_TYPE in scraped_link:
                    links.append(scraped_link)
                    print(f'Found: {scraped_link}')
        print(f'Found {len(links)} files')
        return links
    

class FileTransfer(ABC):

    @abstractmethod
    def transfer_file(self):
        """Transfers the files"""
        pass

    @abstractmethod
    def transfer_prompt(self, transfer_type: str):
        """Asks the user if they want to download the files
        """

        download = input(f'Do you want to {transfer_type} the files? (y/n) ')

        if download == 'y':
            self.transfer_file()
        else:
            print('Ok, bye!')


@dataclass
class DownloadFile(FileTransfer, Scraper):

    url: str = ""

    def transfer_file(self):
        """Downloads the files"""

        site = self.scrape_file_links(self.url)
        #self.scrape_file_size(site)

        # Download files
        for link in tq(site):
            file_link = requests.get(link)
            with open("data/" + link.split('/')[-1], 'wb') as parquet_file:
                parquet_file.write(file_link.content)
                print(f'Downloaded: {link}')

    def transfer_prompt(self):
        super().transfer_prompt("download")


@dataclass
class UploadFile(FileTransfer, AzureContainer):

    file_download: DownloadFile = DownloadFile()

    def get_files(self):
        """Gets the files from the data folder"""

        files = []
        for file in os.listdir('data'):
            if file.endswith(FILE_TYPE):
                files.append(file)
        return files

    def transfer_file(self):

        """Uploads the files"""
        files = self.get_files()

        print('Files already downloaded')
        print('Uploading files...')

        for file in tq(files):
            container_client = self.get_container_client()
            blob_client = container_client.get_blob_client(file)

            file_path = Path(DIR_PATH + file)

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
                print(f'Uploaded to azure: {file_path}')

                os.remove(file_path)
                print(f'Removed: {file_path}')
      
    def transfer_prompt(self):
        super().transfer_prompt("upload")


def main() -> None:
    scraper = Scraper()
    download_file = DownloadFile(URL)
    upload_file = UploadFile()

    download_file.transfer_prompt()
    upload_file.transfer_prompt()


if __name__ == "__main__":
    main()
