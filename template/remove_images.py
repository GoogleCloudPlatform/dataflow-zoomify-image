# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A script for removing tiled images from the cloud
"""

import os
import argparse
import logging
import requests

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

class CloudImageManager():
    """Cloud Image Manager"""
    def __init__(self, images_bucket, tiles_bucket, project, dataset, table):
        self.project = project
        self.dataset = dataset
        self.table = table

        self.client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=128,
            pool_maxsize=128,
            max_retries=3,
            pool_block=True)
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)
        self.bgclient = bigquery.Client(project=self.project)

        self.images_bucket = self.client.bucket(images_bucket)
        self.tiles_bucket = self.client.bucket(tiles_bucket)

    def delete_image(self, imagecode):
        """Deletes image"""
        dataset = self.dataset
        table = self.table
        cmd = (
            f"select path, extension from `{dataset}.{table}` "
            f"where imagecode='{imagecode}' limit 1")
        query_job = self.bgclient.query(cmd)
        rows = query_job.result()
        for row in rows:
            # Delete image
            image_key = (
                row.path + os.path.sep + imagecode + '.' + row.extension)
            print('Deleting image', image_key, 'in', self.images_bucket)
            blob = self.images_bucket.blob(image_key)
            try:
                blob.delete()
            except GoogleCloudError as error:
                print(error)

            # Delete tiles
            tiles_key = row.path + os.path.sep + imagecode
            print('Deleting tiles', tiles_key, 'in', self.tiles_bucket)
            blobs = list(self.tiles_bucket.list_blobs(prefix=tiles_key))
            self.tiles_bucket.delete_blobs(blobs)

            # Delete BigQuery record
            print(
                'Deleting',
                dataset + '.' + table,
                'record with imagecode',
                imagecode)
            cmd = (
                f'delete from `{dataset}.{table}` '
                f'where imagecode="{imagecode}"')
            query_job = self.bgclient.query(cmd)
            _ = query_job.result()

            return
        print('Image', imagecode, 'not found')

def main(argv=None):
    """Main entry point"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--images-bucket',
        dest='images_bucket',
        help='Bucket where images are stored.')
    parser.add_argument(
        "--tiles-bucket",
        dest="tiles_bucket",
        help="Bucket where tiles are stored.")
    parser.add_argument(
        '--bigquery-project',
        dest='bigquery_project',
        help='Bigquery project name.')
    parser.add_argument(
        '--bigquery-dataset',
        dest='bigquery_dataset',
        help='Bigquery dataset name.')
    parser.add_argument(
        '--bigquery-table',
        dest='bigquery_table',
        help='Bigquery table name.')
    parser.add_argument(
        '--images',
        dest='images',
        help='Image codes separated by space.')

    known_args, _ = parser.parse_known_args(argv)

    manager = CloudImageManager(
        known_args.images_bucket,
        known_args.tiles_bucket,
        known_args.bigquery_project,
        known_args.bigquery_dataset,
        known_args.bigquery_table)

    images = known_args.images.split(',')
    for imagecode in images:
        manager.delete_image(imagecode)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
