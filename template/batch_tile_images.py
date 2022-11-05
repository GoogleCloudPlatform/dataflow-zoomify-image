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
import os
import argparse
import logging
import warnings
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor
from typing import Generator, List, NamedTuple
from itertools import count

from PIL import Image, ImageOps, ImageFile, UnidentifiedImageError

import requests

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.gcp import gcsio
from apache_beam.io.gcp import bigquery_file_loads
from apache_beam.io.gcp.internal.clients import bigquery as bq_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.cloud.storage.retry import DEFAULT_RETRY

# Hardcoded tile size
TILE_SIZE = 256

# Custom retry
NUM_RETRIES = 100 # "num_retries" parameter is deprecated!

class ImageWithPath(NamedTuple):
    """ImageWithPath is a NamedTuple indicating a PIL image
    and a path

    Attributes:
        path (str): Path for the image
        image (PIL.Image.Image): A PIL image.
    """
    path: str
    image: Image.Image

# Disable pylint checks that do not work well with Apache Beam
# pylint: disable=abstract-method,attribute-defined-outside-init
# pylint: disable=arguments-differ


def log_message(log_table: str, msg: str):
    issued = datetime.now()
    logging.info(msg)
    bq_client = bigquery.Client()
    query = f"INSERT INTO `{log_table}` (issued, message) VALUES ('{issued}', '{msg}')"
    query_job = bq_client.query(query)
    try:
        query_job.result()
    except GoogleCloudError as error:
        print(error)


class DoFnWithGCSClientMultithread(beam.DoFn):
    """Abstract for GCS Multithread
    """
    def setup(self):
        self.client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=128,
            pool_maxsize=128,
            max_retries=3,
            pool_block=True)
        # pylint: disable=protected-access
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)
    def start_bundle(self):
        self.executor = ThreadPoolExecutor(128)
    def process(self, element):
        pass
    def finish_bundle(self):
        self.executor.shutdown()


class UploadImageToGCS(DoFnWithGCSClientMultithread):
    """UploadImageToGCS is a beam function object that uploads images
    to Google Cloud Storage

    Elements must be instances of ImageWithPath.
    Image must be JPEG.
    """

    def process(self, element: ImageWithPath):
        """Uploads image to GCS."""
        path = element.path
        image_buffer = BytesIO()
        element.image.save(image_buffer, format="jpeg", quality=80)
        bucket_name, target_key = split_path(path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(
                lambda x: blob.upload_from_string(
                    x, content_type="image/jpeg", num_retries=NUM_RETRIES),
                image_buffer.getvalue()
            )
        except GoogleCloudError as gcp_err:
            print(gcp_err)
            gcp_err_str = str(gcp_err)
            msg = f"Unable to upload image {path}: {gcp_error_str}"
            log_message(log_table, msg)
        yield "Ok"


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
                ImageWithPath(
                    file_path, image.crop((left, top, right, bottom)))
            )

    return image_tiles


class GenerateTiles(beam.DoFn):
    """GenerateTiles is a beam function object that generate tiles
    from a image based on Zoomify.
    It also generates ImageProperties.xml for the image and uploads it to GCS.
    """

    def setup(self):
        # pylint: disable=attribute-defined-outside-init
        self.client = storage.Client()
        self.custom_retry = DEFAULT_RETRY.with_deadline(360.0)
        self.custom_retry = self.custom_retry.with_delay(initial=1.0, multiplier=3.0, maximum=300.0)

    def process(
        self,
        element,
        final_bucket: str):
        """Generate tiles."""
        bucket_name, target_key = split_path(element["parameters"]["save_to"])
        bucket = self.client.bucket(bucket_name)
        if element["parameters"]["action"] == "update":
            # Remove existing tiles if they exist
            blobs = list(bucket.list_blobs(prefix=target_key))
            try:
                bucket.delete_blobs(blobs, retry=self.custom_retry)
            except GoogleCloudError as error:
                print(error)
                error_str = str(error)
                tiles_path = element["parameters"]["save_to"]
                msg = f"Failed to delete existing tiles {tiles_path}: {error_str}"
                log_message(log_table, msg)


        # Generate tiers (downscaled images)
        images = [element["parameters"]["image"]]
        width, height = element["parameters"]["image"].size

        while height > TILE_SIZE or width > TILE_SIZE:
            width //= 2
            height //= 2
            images.insert(0, images[-1].resize((width, height), Image.BOX))

        # Tile generator
        i_gen = count()

        for index, image_tier in enumerate(images):
            for tile in tile_image(
                image_tier,
                index,
                i_gen,
                element["parameters"]["save_to"]):
                yield beam.pvalue.TaggedOutput("tiles", tile)

        # Save ImageProperties.xml
        element["parameters"]["img_width"] = (
            element["parameters"]["image"].size[0])
        element["parameters"]["img_height"] = (
            element["parameters"]["image"].size[1])
        del element["parameters"]["image"]

        data = (
            f'<IMAGE_PROPERTIES WIDTH="{element["parameters"]["img_width"]}" '
            f'HEIGHT="{element["parameters"]["img_height"]}" '
            f'NUMTILES="{next(i_gen)}" '
            'NUMIMAGES="1" VERSION="1.8" '
            f'TILESIZE="{TILE_SIZE}"/>'
        )
        output_folder = element["parameters"]["save_to"].replace(
            f"gs://{bucket_name}/","")
        target_key = output_folder + "/ImageProperties.xml"
        blob = bucket.blob(target_key)

        try:
            blob.upload_from_string(data, retry=self.custom_retry)
        except GoogleCloudError as error:
            print(error)
            error_str = str(error)
            properties_path = blob.path
            msg = f"Failed to create {properties_path}: {error_str}"
            log_message(log_table, msg)

        element["parameters"]["modified"] = datetime.now()
        element["parameters"]["path_in_db"] = os.path.split(output_folder)[0]
        img_input_path = element["parameters"]["img_input_path"]
        input_bucket_name, target_key = split_path(img_input_path)

        # Move input image to final destination
        if input_bucket_name != final_bucket:
            input_bucket = self.client.bucket(input_bucket_name)
            input_blob = input_bucket.blob(target_key)
            _ = input_bucket.copy_blob(
                input_blob,
                self.client.bucket(final_bucket),
                target_key)
            try:
                input_blob.delete(retry=self.custom_retry)
            except GoogleCloudError as error:
                print(error)
                error_str = str(error)
                msg = f"Failed to delete input file after job {img_input_path}: {error_str}"
                log_message(log_table, msg)

        yield beam.pvalue.TaggedOutput(
            f"bq_{element['parameters']['action']}s",
            create_bq_dict(element["parameters"])
            )


def split_path(path_str: str):
    """Split bucket name and target key"""
    bucket_name = path_str.replace("gs://", "").split("/")[0]
    target_key = path_str.replace(f"gs://{bucket_name}/","")
    return bucket_name, target_key

def by_extension(element: FileMetadata, extensions: List[str]) -> bool:
    """Check if extension of element is in extensions."""
    ext = element.path.split(".")[-1]
    return ext.lower() in extensions


class ReadImage(beam.DoFn):
    """Reads image from a path in GCS
        and return the other parameters received."""
    def setup(self):
        # pylint: disable=attribute-defined-outside-init
        self.client = storage.Client()
        self.custom_retry = DEFAULT_RETRY.with_deadline(360.0)
        self.custom_retry = self.custom_retry.with_delay(initial=1.0, multiplier=3.0, maximum=300.0)

    def process(self, element, input_path: str, output_path: str, log_table: str):
        img_input_path = element["files"][0].path
        directory, filename = os.path.split(img_input_path)
        name, extension = os.path.splitext(filename)
        img_subpath = directory[len(input_path):]
        if img_subpath:
            # remove leading "/" before joining with output
            img_subpath = img_subpath[1:]
        save_to = os.path.join(output_path, img_subpath, name)
        input_bucket_name, target_key = split_path(img_input_path)
        blob = self.client.bucket(input_bucket_name).get_blob(target_key, retry=self.custom_retry)

        # Read image
        # Ignore DecompressionBombWarning
        Image.MAX_IMAGE_PIXELS = None
        warnings.simplefilter('ignore', Image.DecompressionBombWarning)
        # Avoid interrupting the flow with truncated images
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        gcs = gcsio.GcsIO()
        image = None
        # Manually retry, as there doesn't seem to be a way to configue retries on gcs.open
        n = 0
        while n < NUM_RETRIES:
            try:
                image = Image.open(gcs.open(img_input_path)).convert("RGB")
                break
            except UnidentifiedImageError:
                pass
            except FileNotFoundError:
                pass
            n += 1
        if image is None:
            msg = f"Unable to read image: {img_input_path}"
            log_message(log_table, msg)
            yield None
        image = ImageOps.exif_transpose(image)
        element["parameters"] = {
            "image": image,
            "save_to": save_to,
            "md5": blob.md5_hash,
            "img_input_path": img_input_path,
            "extension": extension.split('.')[-1], # remove leading dot from extension
            "imagecode": name,
            "barcode": name.split("_")[0],
            "action": "update" if element["bq_metadata"] else "insert"
            }

        yield element

class CheckMD5(beam.DoFn):
    """Checks MD5 hash"""
    def setup(self):
        self.client = storage.Client()
        self.custom_retry = DEFAULT_RETRY.with_deadline(360.0)
        self.custom_retry = self.custom_retry.with_delay(initial=1.0, multiplier=3.0, maximum=300.0)

    def process(self, element, final_bucket: str, log_table: str):
        # Get image names
        img_input_path = element["files"][0].path
        # Compare input image md5 with bq
        input_bucket_name, target_key = split_path(img_input_path)
        input_bucket = self.client.bucket(input_bucket_name)
        blob = input_bucket.get_blob(target_key, retry=self.custom_retry)
        if blob is None:
            # TODO: figure out why sometimes the blob being processed doesn't exist (??)
            msg = f"Could not find image being processed {img_input_path}"
            log_message(log_table, msg)
            yield None
        if blob.md5_hash != element["bq_metadata"][0]["md5"]:
            element["md5"] = True
            if input_bucket_name != final_bucket:
                # Remove existing image that will be updated
                orig_key = element["bq_metadata"][0]["path"] + '/' + element["bq_metadata"][0]["filename"]
                orig_blob = self.client.bucket(final_bucket).get_blob(orig_key, retry=self.custom_retry)
                if orig_blob:
                    try:
                        orig_blob.delete(retry=self.custom_retry)
                    except GoogleCloudError as error:
                        print(error)
                        error_str = str(error)
                        msg = f"Failed to delete image to be updated {final_bucket}/{orig_key}: {error_str}"
                        log_message(log_table, msg)
                else:
                    msg = f"Could not find outdated image to be deleted {final_bucket}/{orig_key}"
                    log_message(log_table, msg)
        else:
            element["md5"] = False
            if input_bucket_name != final_bucket:
                # Remove input image identical to existing image
                try:
                    blob.delete(retry=self.custom_retry)
                    msg = f"Discarded identical image: {img_input_path}"
                    log_message(log_table, msg)
                except GoogleCloudError as error:
                    print(error)
                    error_str = str(error)
                    msg = f"Failed to discard identical image {img_input_path}: {error_str}"
                    log_message(log_table, msg)
        yield element


def update_table(_, bq_table, bq_update_table):
    """Updates table with a temp update table."""
    bq_client = bigquery.Client()

    # Updates table with temp table
    query = (
        f'UPDATE `{bq_table}` o '
        'SET extension=u.extension, path=u.path, '
        'width=u.width, height=u.height, md5=u.md5, '
        'modified=u.modified '
        f'FROM `{bq_update_table}` u '
        f'WHERE o.imagecode=u.imagecode'
    )
    query_job = bq_client.query(query)
    try:
        query_job.result()
    except GoogleCloudError as error:
        print(error)

    # Drops temp table
    query = f"DROP TABLE IF EXISTS `{bq_update_table}`"
    query_job = bq_client.query(query)

    try:
        query_job.result()
    except GoogleCloudError as error:
        print(error)

    return "Ok"

def create_bq_dict(parameters):
    """Create BigQuery Dict from parameters"""
    return {
        "barcode": parameters["barcode"],
        "imagecode": parameters["imagecode"],
        "extension": parameters["extension"],
        "path": parameters["path_in_db"],
        "width": parameters["img_width"],
        "height": parameters["img_height"],
        "md5": parameters["md5"],
        "modified": parameters["modified"],
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
        '--bigquery-log-table',
        dest='bigquery_log_table',
        help='Bigquery log table name.')
    parser.add_argument(
        '--final-bucket',
        dest='final_bucket',
        help='Final bucket destination for input images.')
    parser.add_argument(
        '--initial-import',
        dest='initial_import',
        action='store_true',
        default=False,
        help='Avoid checking BigQuery whether image already exists.')
    parser.add_argument(
        '--bq-temp-suffix',
        dest='bq_temp_suffix',
        default="temp",
        help='Suffix for temporary BigQuery table.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    valid_extensions = known_args.extensions.split(",")

    bigquery_table = (
        f"{known_args.bigquery_project}."
        f"{known_args.bigquery_dataset}."
        f"{known_args.bigquery_table}"
    )

    bigquery_log_table = (f"{known_args.bigquery_log_table}")

    if '.' not in known_args.bigquery_log_table:
        bigquery_log_table = (
            f"{known_args.bigquery_project}."
            f"{known_args.bigquery_dataset}."
            f"{known_args.bigquery_log_table}"
        )

    # pylint: disable=protected-access
    bigquery_table_schema = bigquery.Client().get_table(
        bigquery_table)._properties["schema"]

    bigquery_update_table = f"{bigquery_table}_{known_args.bq_temp_suffix}"


    bigquery_query = (
        "SELECT imagecode, "
        "CONCAT(imagecode, '.', extension) "
        "AS filename, path, md5 "
        f"FROM `{bigquery_table}`")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the text file[pattern] into a PCollection.
        files = (
            pipeline
            | "List files from the input path" >> MatchFiles(known_args.input)
            | "Filter by file extension"
            >> beam.Filter(by_extension, valid_extensions)
            | "Create imagecode key for files"
            >> beam.Map(
                lambda x: (
                    os.path.splitext(os.path.split(x.path)[1])[0],
                    x
                    )
                )
            )

        # Read existing metadata from Bigquery
        bq_metadata = (
            pipeline
            | beam.Create([])
            ) if known_args.initial_import else (
            pipeline
            | "Read Metadata from Bigquery" >> beam.io.ReadFromBigQuery(
                query=bigquery_query, use_standard_sql=True)
            | "Create imagecode key for each row" >> beam.Map(
                lambda x: (x["imagecode"], x))
        )

        # Check which files have their metadata on BigQuery
        files_not_in_bq, files_in_bq= (
            {"files": files, "bq_metadata": bq_metadata}
            | "Join files and metadata" >> beam.CoGroupByKey()
            | "Remove key after join" >> beam.MapTuple(lambda _, v: v)
            | "Filter joins with no file" >> beam.Filter(lambda x: x["files"])
            | "Partition" >> beam.Partition(
                lambda x, _: 1 if x["bq_metadata"] else 0, 2)
        )

        # Check which files with metadata in BigQuery are new
        files_in_bq_new = (
            files_in_bq
            | "Check MD5" >> beam.ParDo(CheckMD5(), known_args.final_bucket, bigquery_log_table)
            | 'Filter out missing files' >> beam.Filter(lambda x: x is not None)
            | "Filter same files" >> beam.Filter(lambda x: x["md5"])
        )

        # Merge all new files in a PCollection
        new_files = (
            (files_not_in_bq, files_in_bq_new)
            | "Merge Updates and Inserts" >> beam.Flatten())

        # Tile images -> Image Tiles and BigQuery metadata
        tiling_results = (
            new_files
            | "Read images to memory" >> beam.ParDo(
                ReadImage(),
                input_path=os.path.split(known_args.input)[0],
                output_path=known_args.output,
                log_table=bigquery_log_table)
            | 'Filter out unread files' >> beam.Filter(lambda x: x is not None)
            | "Generate tiles" >> beam.ParDo(
                GenerateTiles(),
                known_args.final_bucket
                ).with_outputs(
                    "tiles",
                    "bq_inserts",
                    "bq_updates"
                )
        )

        # Upload tiles to GCS
        _ = (
            tiling_results["tiles"]
            | "Reshuffle" >> beam.Reshuffle()
            | "Upload tiles to GCS" >> beam.ParDo(UploadImageToGCS())
        )

        # Insert new files metadata into Bigquery
        _ = (
            tiling_results["bq_inserts"]
            | "Insert tile metadata to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=known_args.bigquery_table,
                dataset=known_args.bigquery_dataset,
                project=known_args.bigquery_project
                )
        )

        # Update existing metadata in Bigquery
        job_ids  = (
            tiling_results["bq_updates"]
            | "Insert tile metadata to update table"
            >> beam.io.WriteToBigQuery(
                table=(
                    f"{known_args.bigquery_table}_{known_args.bq_temp_suffix}"
                ),
                dataset=known_args.bigquery_dataset,
                project=known_args.bigquery_project,
                schema=bq_beam.TableSchema(
                    fields=bigquery_table_schema["fields"])
                )
        )

        update_load_jobs_ids = (
            pipeline
            | "ImpulseMonitorUpdateLoads" >> beam.Create([None])
            | "WaitForUpdateDestinationLoadJobs" >> beam.ParDo(
                bigquery_file_loads.WaitForBQJobs(),
                beam.pvalue.AsList(job_ids['destination_load_jobid_pairs']))
        )
        update_copy_jobs_ids = (
            pipeline
            | "ImpulseMonitorUpdateCopies" >> beam.Create([None])
            | "WaitForUpdateDestinationCopyJobs" >> beam.ParDo(
                bigquery_file_loads.WaitForBQJobs(),
                beam.pvalue.AsList(job_ids['destination_copy_jobid_pairs']))
        )
        _ = (
            (update_load_jobs_ids, update_copy_jobs_ids)
            | "Merge completed update ids" >> beam.Flatten()
            | "Count ids" >> beam.combiners.Count.Globally()
            | "Update Bigquery Table" >> beam.Map(
                update_table, bigquery_table, bigquery_update_table)
        )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
