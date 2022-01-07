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

"""A workflow for tiling images
"""
from io import BytesIO
import math
import os
import argparse
import logging

from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
from typing import Generator, List, NamedTuple
from itertools import count

from PIL import Image, ImageOps

import requests

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.gcp import gcsio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

TILE_SIZE = 256

class ImageWithPath(NamedTuple):
    """ImageWithPath is a NamedTuple indicating a PIL image
    and a path

    Attributes:
        path (str): Path for the image
        image (PIL.Image.Image): A PIL image.
    """
    path: str
    image: Image.Image


class DoFnWithGCSClient(ABC, beam.DoFn):
    def setup(self):
        self.client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3, pool_block=True)
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)
    def start_bundle(self):
        self.executor = ThreadPoolExecutor(128)
    @abstractmethod
    def process(self, el):
        pass
    def finish_bundle(self):
        self.executor.shutdown()


class FilterByStatus(DoFnWithGCSClient):
    """FilterByStatus is a beam function object that checks
    if the image being processed was already imported by looking
    up the image code (file name) on a bigquery table. New images
    proceed with the data flow with action "insert" (to be included
    in the bigquery table in the end). Images that already exist 
    in the database get their md5 compared with the previous md5.
    If they are the same, the image is deleted and the data flow
    interrupted. If the md5 is different, data flow proceeds with
    action "update".

    Elements are file objects from where we get the path.
    Instances are initiated with:
    output: output bucket name
    input_dir: directory where file resides in input bucket
    project: bigquery project name
    dataset: bigquery dataset name
    table: bigquery table name
    final_bucket: bucket name to move imported images
    """
    def __init__(self, output: str, input_dir: str, project: str, dataset: str, table: str, final_bucket: str):
        self.output = output
        self.input_dir = input_dir
        self.project = project
        self.dataset = dataset
        self.table = table
        self.final_bucket = final_bucket
    def setup(self):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        #super().setup()
        DoFnWithGCSClient.setup(self)
        self.bgclient = bigquery.Client(project=self.project)
    def process(self, el):
        # Get image names
        path = el.path
        directory, filename = os.path.split(path)
        name, ext = os.path.splitext(filename)
        imagecode = name
        subpath = directory[len(self.input_dir):]
        if subpath:
           # remove leading "/" before joining with output
           subpath = subpath[1:]
        save_to = self.output + subpath + "/" + name
        # Get input image md5
        bucket_name, target_key = split_path(path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.get_blob(target_key)
        input_md5 = blob.md5_hash
        dataset = self.dataset
        table = self.table
        # Check if image was already imported
        cmd = f"select filename, path, md5 from `{dataset}.{table}` where imagecode='{imagecode}' limit 1"
        query_job = self.bgclient.query(cmd)
        rows = query_job.result()
        action = 'insert'
        for row in rows:
            action = 'update'
            # Compare with recorded md5
            if row.md5 == input_md5:
                # ignore unchanged images, removing them
                blob.delete()
                return
            # delete old version of the image (which can be in another location!)
            orig_name, orig_ext = os.path.splitext(row.filename)
            orig_key = row.path + orig_ext
            final_bucket = self.client.bucket(self.final_bucket)
            orig_blob = final_bucket.blob(orig_key)
            try:
                orig_blob.delete()
            except Exception as e:
                print(e)
            break
        # Only process new or changed images
        yield{
            "input_path": path, 
            "save_to": save_to,
            "filename": filename,
            "ext": ext,
            "imagecode": imagecode,
            "barcode": imagecode.split("_")[0],
            "md5": input_md5, 
            "action": action
        }


class UploadImageToGCS(DoFnWithGCSClient):
    """UploadImageToGCS is a beam function object that uploads images
    to Google Cloud Storage

    Elements must be instances of ImageWithPath.
    Image must be JPEG.
    """

    def process(self, element: ImageWithPath, *args, **kwargs):
        """Uploads image to GCS."""
        path = element.path
        image_buffer = BytesIO()
        element.image.save(image_buffer, format="jpeg")
        bucket_name = path.replace("gs://", "").split("/")[0]
        bucket = self.client.bucket(bucket_name)
        target_key = path.replace(f"gs://{bucket_name}/", "")
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(
                lambda x: blob.upload_from_string(x, content_type="image/jpeg"),
                image_buffer.getvalue()
            )
        except GoogleCloudError as gcp_err:
            print(gcp_err)
        return ["Ok"]


def tile_image(
    image: Image.Image,
    tier: int,
    gen: Generator,
    prefix: str) -> List[ImageWithPath]:
    """Create tiles with path from a PIL Image
    Args:
        image (PIL.Image.Image): A PIL Image
        tier (int): Current tier
        gen (Generator): TileGroup counter

    Returns:
        image_tiles (List[ImageWithPath]) : A list of tiles with path.
    """
    image_tiles = []

    for top in range(0, image.size[1], TILE_SIZE):
        bottom = min(image.size[1], top + TILE_SIZE)
        for left in range(0, image.size[0], TILE_SIZE):
            right = min(image.size[0], left + TILE_SIZE)
            file_path = (
                f"{prefix}/TileGroup{next(gen) // TILE_SIZE}"
                f"/{tier}-{left // TILE_SIZE}-{top // TILE_SIZE}.jpg"
            )
            image_tiles.append(
                ImageWithPath(file_path, image.crop((left, top, right, bottom)))
            )

    return image_tiles

class GenerateTiles(DoFnWithGCSClient):
    """GenerateTiles is a beam function object that generate tiles
    from a image based on Zoomify.
    It also generates ImageProperties.xml for the image and uploads it to GCS.
    """

    def __init__(self, project, dataset, table, final_bucket):
        self.project = project
        self.dataset = dataset
        self.table = table
        self.final_bucket = final_bucket

    def setup(self):
        """Starts GCS client and BigQuery client."""
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        #super().setup()
        DoFnWithGCSClient.setup(self)
        self.bgclient = bigquery.Client(project=self.project)

    def process(self, el):
        """Generate tiles."""
        im = el["image"]
        save_to = el["save_to"]
        # Remove existing tiles if they exist
        bucket_name, target_key = split_path(save_to)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(blob.delete)
        except Exception as e:
            print(e)

        # Generate tiers (downscaled images)
        images = []
        tier = math.ceil(
            max([math.log(shape / TILE_SIZE, 2) for shape in im.size])
            )
        for i in range(tier, 0, -1):
            size = (
                im.size[0] // 2 ** i,
                im.size[1] // 2 ** i
                )
            images.append(im.resize(size, Image.BOX))
        images.append(im)

        # Tile generator
        i_gen = count()
        try:
            for index, image_tier in enumerate(images):
                for image_with_path in tile_image(image_tier, index, i_gen, save_to):
                    yield image_with_path
        finally:
            # Save ImageProperties.xml
            img_width = im.size[0]
            img_height = im.size[1]
            num_tiles = next(i_gen)
            data = f'<IMAGE_PROPERTIES WIDTH="{img_width}" HEIGHT="{img_height}" NUMTILES="{num_tiles}" NUMIMAGES="1" VERSION="1.8" TILESIZE="{TILE_SIZE}"/>'
            output_folder = save_to.replace(f"gs://{bucket_name}/","")
            target_key = output_folder + "/ImageProperties.xml"
            blob = bucket.blob(target_key)
            try:
                self.executor.submit(blob.upload_from_string, data)
            except Exception as e:
                print(e)
            # Save image metadata in db
            barcode = el["barcode"]
            imagecode = el["imagecode"]
            filename = el["filename"]
            md5 = el["md5"]
            dataset = self.dataset
            table = self.table
            if el["action"] == "insert":
                cmd = f'insert into `{dataset}.{table}` (barcode, imagecode, filename, path, width, height, md5) values("{barcode}", "{imagecode}", "{filename}", "{output_folder}", {img_width}, {img_height}, "{md5}")'
            else:
                cmd = f'update `{dataset}.{table}` set filename="{filename}", path="{output_folder}", width={img_width}, height={img_height}, md5="{md5}" where imagecode="{imagecode}"'
            query_job = self.bgclient.query(cmd)
            result = query_job.result()
            # Move input image to final destination
            input_bucket_name, target_key = split_path(el["input_path"])
            input_bucket = self.client.bucket(input_bucket_name)
            input_blob = input_bucket.blob(target_key)
            final_bucket = self.client.bucket(self.final_bucket)
            blob_copy = input_bucket.copy_blob(input_blob, final_bucket, target_key)
            try:
                self.executor.submit(input_blob.delete)
            except Exception as e:
                print(e)


def split_path(path):
    bucket_name = path.replace("gs://", "").split("/")[0]
    target_key = path.replace(f"gs://{bucket_name}/","")
    return bucket_name, target_key

def by_extension(element: FileMetadata, extensions: List[str]) -> bool:
    """Check if extension of element is in extensions."""
    ext = element.path.split(".")[-1]
    return ext.lower() in extensions


def img_read(el):
    """Reads image from a path in GCS and return the other parameters received."""
    path = el["input_path"]
    gcs = gcsio.GcsIO()
    image = Image.open(gcs.open(path)).convert("RGB")
    image = ImageOps.exif_transpose(image)

    return {"image": image,
            "save_to": el["save_to"],
            "md5": el["md5"],
            "input_path": path,
            "filename": el["filename"],
            "imagecode": el["imagecode"],
            "barcode": el["barcode"],
            "action": el["action"]
           }


def main(argv=None, save_main_session=True):
    """Main entry point"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://YOUR_INPUT_BUCKET/AND_INPUT_PREFIX/",
        help="Input folder to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX",
        help="Output file to write results to.",
    )
    parser.add_argument(
        "--extensions",
        dest="extensions",
        default="jpg",
        help="File extensions to be processed.",
    )
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
        '--final-bucket',
        dest='final_bucket',
        help='Final bucket destination for input images.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    valid_extensions = known_args.extensions.split(",")

    input_dir, _ = os.path.split(known_args.input)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the text file[pattern] into a PCollection.
        files = pipeline | MatchFiles(known_args.input)
        images = (
            files
            | "Filter by file extension" >> beam.Filter(by_extension, valid_extensions)
            | 'Filter by file status' >> beam.ParDo(FilterByStatus(known_args.output, input_dir, known_args.bigquery_project, known_args.bigquery_dataset, known_args.bigquery_table, known_args.final_bucket))
            | "Read images to memory"
            >> beam.Map(lambda x: img_read(x)))
        tiles = images | "Tile images" >> beam.ParDo(GenerateTiles(known_args.bigquery_project, known_args.bigquery_dataset, known_args.bigquery_table, known_args.final_bucket))
        _ = tiles | "Upload to GCS" >> beam.ParDo(UploadImageToGCS())


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
