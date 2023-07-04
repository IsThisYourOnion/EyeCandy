import os
import json
from PIL import Image
import requests
from io import BytesIO
import zipfile
import gdown
import subprocess
from config import config
import sys


class FetchData:
    def __init__(self, paths, urls):
        self.paths = paths
        self.urls = urls

    def _is_extracted(self, path):
        return os.path.exists(path)

    def download_and_unzip(self, url, extract_to='.'):
        zip_file_path = url.split('/')[-1]
        if self._is_extracted(extract_to):
            print(f"Data from {url} already exists in {extract_to}, skipping download.")
            return
        response = requests.get(url)
        with open(zip_file_path, 'wb') as file:
            file.write(response.content)
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        os.remove(zip_file_path)

    def download_datasets(self):
        for url in self.urls:
            self.download_and_unzip(url, extract_to='./data/UMN/')

    def process_dataset(self, dataset_path):
        dataset_dir = os.path.dirname(dataset_path)
        print(f"Processing dataset: {dataset_path}")
        if not os.path.exists(dataset_path):
            print(f"Dataset {dataset_path} doesn't exist.")
            return
        with open(dataset_path, 'r') as f:
            annotations = json.load(f)
            for i, image in enumerate(annotations['images']):
                file_name = image['file_name']
                url_original = image['flickr_url']
                file_path = os.path.join(dataset_dir, file_name)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                if not os.path.isfile(file_path):
                    response = requests.get(url_original)
                    img = Image.open(BytesIO(response.content))
                    img.save(file_path, exif=img.info["exif"] if img._getexif() else None)
                self._progress_bar(i, len(annotations['images']))
            print('Finished')

    def _progress_bar(self, i, total, bar_size=30):
        x = int(bar_size * i / total)
        sys.stdout.write("%s[%s%s] - %i/%i\r" % ('Loading: ', "=" * x, "." * (bar_size - x), i, total))
        sys.stdout.flush()

    def process_datasets(self):
        for path in self.paths:
            self.process_dataset(path)

    def download_from_drive(self, url, output, extract_to):
        if self._is_extracted(extract_to):
            print(f"Data from {url} already exists in {extract_to}, skipping download.")
            return
        gdown.download(url, output, quiet=False)
        self.download_and_unzip(output, extract_to)

    def download_from_kaggle(self, dataset, extract_to='./data/kaggle_waste/'):
        if self._is_extracted(extract_to):
            print(f"Data from {dataset} already exists in {extract_to}, skipping download.")
            return
        subprocess.run(['kaggle', 'datasets', 'download', '-d', dataset])
        self.download_and_unzip(dataset + '.zip', extract_to)


def pullDatasets():
    taco_paths = config.taco_paths
    umn_urls = config.umn_urls
    processor = FetchData(taco_paths, umn_urls)
    processor.download_datasets()
    processor.process_datasets()
    google_url = config.google_url
    google_output = config.google_output
    processor.download_from_drive(google_url, google_output, "./data/mju")
    kaggle_dataset = config.kaggle_datasets
    for kaggle_data in kaggle_dataset:
        processor.download_from_kaggle(kaggle_data)
