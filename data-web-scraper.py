import asyncio
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm as tq

from azure_storage.azure_storage import AzureContainer

class BeautifulSoup:

    def _get_soup(self, url: str) -> bs:
        """Gets the soup from the website"""

        return bs(requests.get(url).text, 'html.parser')

@dataclass
class Scraper(BeautifulSoup):

    url: str
    date_range: tuple 
    file_type: str

    def scrape_file_links(self) -> list:
        """Scrapes the file links from the website"""

        # Get taxi type
        taxi_type = input('What type of taxi do you want to download? (yellow, green, fhv) ')

        # Get date range
        start_date = self.date_range[0]
        end_date = self.date_range[1]

        # Get file links
        links = []

        print('Getting file links...')
        for link in self._get_soup(self.url).find_all('a'):
            for date in range(start_date, end_date + 1):
                scraped_link = link.get('href')
                if str(date) in scraped_link and self.file_type in scraped_link and taxi_type in scraped_link:
                    links.append(scraped_link)
                    print(f'Found: {scraped_link}')
        print(f'Found {len(links)} files')
        return links
    
    
@dataclass
class FileTransfer(ABC, AzureContainer):

    directory_path: str
    file_links: list 
    content_settings = AzureContainer.get_azure_content_settings
    container_client = AzureContainer.get_container_client

    @abstractmethod
    async def transfer_file(self):
        """Transfers the files"""
        pass

    async def transfer_prompt(self, transfer_type: str):
        """Asks the user if they want to download the files
        """

        download = input(f'Do you want to {transfer_type} the files? (y/n) ')

        if download == 'y':
            await self.transfer_file()
        else:
            print('Ok, bye!')


class DownloadFile(FileTransfer):

    async def transfer_file(self):
        """Downloads the files"""

        # Download files
        for link in tq(self.file_links):
            file_link = requests.get(link)
            with open("data/" + link.split('/')[-1], 'wb') as parquet_file:
                parquet_file.write(file_link.content)

                # 5 second delay to avoid throttling
                await asyncio.sleep(5)
                print("Cooling down...")

                print(f'Downloaded: {link}')

    async def transfer_prompt(self, transfer_type: str):
        await super().transfer_prompt(transfer_type)


@dataclass
class UploadFile(FileTransfer):

    async def transfer_file(self):
        """Uploads the files"""

        print('Uploading files...')

        for file in tq(self.file_links):
            blob_client = self.get_container_client().get_blob_client(file)

            # 5 second delay to avoid throttling
            await asyncio.sleep(5)
            print("Cooling down...")

            blob_client.upload_blob_from_url(file, overwrite=True)
            print(f'Uploaded to azure: {file}')

    async def transfer_prompt(self, transfer_type: str):
        await super().transfer_prompt(transfer_type)

    async def upload_from_local(self):
        """Uploads the files from the local directory"""

        for file in tq(self.file_links):
            blob_client = self.container_client.get_blob_client(file)

            file_path = Path(self.directory_path + file)

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
                print(f'Uploaded to azure: {file_path}')

                await asyncio.sleep(5)
                print("Cooling down...")

                os.remove(file_path)
                print(f'Removed: {file_path}')
    
    
# Gets the date range
def get_date_range() -> tuple:
    """Gets the date range"""

    start_date = int(input('What year do you want to start from? '))
    end_date = int(input('What year do you want to end at? '))
    return (start_date, end_date)

async def main() -> None:

    # Constants
    URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    FILE_TYPE = '.parquet'
    DIR_PATH = 'data/'

    # Gets date range of files to scrape
    date_range = get_date_range()

    # Data scrapers
    scraper = Scraper(date_range=date_range, url=URL, file_type=FILE_TYPE)
    file_links = scraper.scrape_file_links()

    # File transfers
    download_file = DownloadFile(directory_path=DIR_PATH, file_links=file_links)
    upload_file = UploadFile(directory_path=DIR_PATH, file_links=file_links)

    # Prompts
    #await download_file.transfer_prompt("download")
    await upload_file.transfer_prompt("upload")


if __name__ == "__main__":
    asyncio.run(main())
