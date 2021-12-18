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


class UploadImageToGCS(beam.DoFn):
    """UploadImageToGCS is a beam function object that uploads images
    to Google Cloud Storage

    Elements must be instances of ImageWithPath.
    Image must be JPEG.
    """

    def setup(self):
        """Creates multiple GCS threads."""
        self.client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=128, pool_maxsize=128, max_retries=3, pool_block=True
        )
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)

    def start_bundle(self):
        """Creates a thread pool."""
        self.executor = ThreadPoolExecutor(128)

    def finish_bundle(self):
        """Shutdown thread pool."""
        self.executor.shutdown()

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

class GenerateTiles(beam.DoFn):
    """GenerateTiles is a beam function object that generate tiles
    from a image based on Zoomify.
    It also generates ImageProperties.xml for the image and uploads it to GCS.
    """

    def setup(self):
        """Starts GCS client."""
        self.client = storage.Client()

    def process(self, element: ImageWithPath, *args, **kwargs):
        """Generate tiles."""
        # Extract bucket name and target key
        bucket_name = element.path.replace("gs://", "").split("/")[0]
        bucket = self.client.bucket(bucket_name)

        # Generate tiers (downscaled images)
        images = []
        tier = math.ceil(
            max([math.log(shape / TILE_SIZE, 2) for shape in element.image.size])
            )
        for i in range(tier, 0, -1):
            size = (
                element.image.size[0] // 2 ** i,
                element.image.size[1] // 2 ** i
                )
            images.append(element.image.resize(size, Image.BOX))
        images.append(element.image)

        # Tile generator
        i_gen = count()
        for index, image_tier in enumerate(images):
            for image_with_path in tile_image(image_tier, index, i_gen, element.path):
                yield image_with_path

        # Save ImageProperties.xml to GCS
        blob = bucket.blob(
            element.path.replace(f"gs://{bucket_name}/", "") + "/ImageProperties.xml")
        try:
            blob.upload_from_string(
                f'<IMAGE_PROPERTIES WIDTH="{element.image.size[0]}"'
                f' HEIGHT="{element.image.size[1]}"'
                f' NUMTILES="{next(i_gen)}"'
                ' NUMIMAGES="1" VERSION="1.8"'
                f' TILESIZE="{TILE_SIZE}"/>')
        except GoogleCloudError as gcp_err:
            print(gcp_err)


def by_extension(element: FileMetadata, extensions: List[str]) -> bool:
    """Check if extension of element is in extensions."""
    ext = element.path.split(".")[-1]
    return ext.lower() in extensions


def img_read(path: str, output: str, input_dir: str) -> ImageWithPath:
    """Reads image from a path in GCS and generate an output path for tiles."""
    directory, filename = os.path.split(path)
    name, _ = os.path.splitext(filename)
    subpath = directory[len(input_dir) :]
    if subpath:
        # remove leading "/" before joining with output
        subpath = subpath[1:]
    save_to = output + subpath + "/" + name
    gcs = gcsio.GcsIO()
    image = Image.open(gcs.open(path)).convert("RGB")
    image = ImageOps.exif_transpose(image)
    image_with_path = ImageWithPath(path=save_to, image=image)

    return image_with_path


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
            | "Read images to memory"
            >> beam.Map(lambda x: img_read(x.path, known_args.output, input_dir)))
        tiles = images | "Tile images" >> beam.ParDo(GenerateTiles())
        _ = tiles | "Upload to GCS" >> beam.ParDo(UploadImageToGCS())


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
