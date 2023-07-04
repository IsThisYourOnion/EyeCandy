import os
import os.path
import json
from PIL import Image
import requests
from io import BytesIO
import sys
import zipfile
import gdown
import subprocess
from config import config


class FetchData:
    def __init__(self, paths, urls):
        self.paths = paths
        self.urls = urls

    def download_and_unzip(self, url, extract_to='.'):
        zip_file_path = url.split('/')[-1]
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

        # Load annotations
        with open(dataset_path, 'r') as f:
            annotations = json.loads(f.read())
            nr_images = len(annotations['images'])
            for i in range(nr_images):
                image = annotations['images'][i]

                file_name = image['file_name']
                url_original = image['flickr_url']

                file_path = os.path.join(dataset_dir, file_name)

                # Create subdir if necessary
                subdir = os.path.dirname(file_path)
                if not os.path.isdir(subdir):
                    os.mkdir(subdir)

                if not os.path.isfile(file_path):
                    # Load and Save Image
                    response = requests.get(url_original)
                    img = Image.open(BytesIO(response.content))
                    if img._getexif():
                        img.save(file_path, exif=img.info["exif"])
                    else:
                        img.save(file_path)

                # Show loading bar
                bar_size = 30
                x = int(bar_size * i / nr_images)
                sys.stdout.write("%s[%s%s] - %i/%i\r" % ('Loading: ', "=" * x, "." * (bar_size - x), i, nr_images))
                sys.stdout.flush()
                i+=1

            sys.stdout.write('Finished\n')

    def process_datasets(self):
        print('Note. If for any reason the connection is broken. Just call me again and I will start where I left.')
        for path in self.paths:
            self.process_dataset(path)

    def download_from_drive(self, url, output, extract_to):
        gdown.download(url, output, quiet=False)
        with zipfile.ZipFile(output, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        os.remove(output)

    def download_from_kaggle(self, dataset, extract_to='./data/kaggle_waste/'):
        subprocess.run(['kaggle', 'datasets', 'download', '-d', dataset])
        zip_file_path = dataset.split('/')[-1] + '.zip'
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        os.remove(zip_file_path)



def pullPatasets():
    # Define your dataset paths and download urls.
    taco_paths = config.taco_paths
    umn_urls = config.umn_urls
    # Create an instance of FetchData
    processor = FetchData(taco_paths, umn_urls)
    # Download the datasets
    processor.download_datasets()

    # Process the datasets
    processor.process_datasets()

    # Download from Google Drive
    google_url = config.google_url
    google_output = config.google_output
    processor.download_from_drive(google_url, google_output, "./data/mju")

    # Download from Kaggle
    kaggle_dataset = config.kaggle_datasets
    for kaggle_data in kaggle_dataset:
        processor.download_from_kaggle(kaggle_data)

pullPatasets()