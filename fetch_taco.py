import os
import os.path
import argparse
import json
from PIL import Image
import requests
from io import BytesIO
import sys
import zipfile

class FetchData:
    def __init__(self, paths):
        self.paths = paths

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

def download_and_unzip(urls, extract_to='.'):
    for url in urls:
        zip_file_path = url.split('/')[-1]  # Use the last part of the URL as a temporary filename
        response = requests.get(url)
        with open(zip_file_path, 'wb') as file:
            file.write(response.content)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

        # Remove the zip file as it's no longer needed
        os.remove(zip_file_path)

# Download and unzip the datasets
urls = ["https://conservancy.umn.edu/bitstream/handle/11299/214366/trash_ICRA19.zip?sequence=12&isAllowed=y",
        "https://conservancy.umn.edu/bitstream/handle/11299/214865/dataset.zip?sequence=12&isAllowed=y"]
download_and_unzip(urls, extract_to='./data/UMN/')

# Define your dataset paths. You might need to adjust this according to the structure of the unzipped datasets.
paths = ['./data/taco/taco_annotations.json', './data/uav/uav_annotations.json']

# Create an instance of DatasetProcessor
processor = FetchData(paths)

# Call process_datasets to start processing
processor.process_datasets()
