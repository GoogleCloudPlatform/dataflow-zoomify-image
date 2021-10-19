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

import io
import argparse
import logging
import math
import itertools
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod

import imageio
import requests

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.gcp import gcsio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.core import ParDo

from google.cloud import storage


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

class UploadGCS(DoFnWithGCSClient):
    def process(self, el):
        path = el[0]
        data = el[1]
        bucket_name = path.replace("gs://", "").split("/")[0]
        bucket = self.client.bucket(bucket_name)
        target_key = path.replace(f"gs://{bucket_name}/","")
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(blob.upload_from_string, data)
        except Exception as e:
            print(e)
        return ["Ok"]

class ProcessTiles(DoFnWithGCSClient):
    def process(self, el):
        im = el["image"]
        output = el["output"]
        file_name = el["name"]
        tile_size = 256
        num_tiles = 0
        tier = math.ceil(max([math.log(shape/tile_size, 2) for shape in im.shape]))
        max_step = 2 ** tier
        steps = [int(max_step/2**i) for i in range(tier + 1)]
        tiers = [im[::step, ::step] for step in steps]
        def next_i():
            i = 0
            while i < tile_size:
                yield i // tile_size
                i += 1
        tile_group = next_i()
        for tier_number, tier in enumerate(tiers):
            for item in itertools.product([i for i in range(math.ceil(tier.shape[0]/tile_size))], [i for i in range(math.ceil(tier.shape[1]/tile_size))]):
                num_tiles += 1
                yield{
                    "tier": tier,
                    "tier_number": tier_number,
                    "item": item,
                    "tile_group": next(tile_group),
                    "output": output,
                    "name": file_name
                }
        # Save ImageProperties.xml
        img_width = im.shape[1]
        img_height = im.shape[0]
        data = f'<IMAGE_PROPERTIES WIDTH="{img_width}" HEIGHT="{img_height}" NUMTILES="{num_tiles}" NUMIMAGES="1" VERSION="1.8" TILESIZE="{tile_size}"/>'
        path = f"{output}{file_name}/ImageProperties.xml"
        bucket_name = path.replace("gs://", "").split("/")[0]
        bucket = self.client.bucket(bucket_name)
        target_key = path.replace(f"gs://{bucket_name}/","")
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(blob.upload_from_string, data)
        except Exception as e:
            print(e)


def by_extension(el, extensions):
  ext = el.path.split('.')[-1]
  return ext in extensions

def img_read(path, output):
    kwargs = {}
    ext = path.split('.')[-1]
    if ext == 'png':
        kwargs['pilmode'] = 'RGB' # discard alpha channel
    gcs = gcsio.GcsIO()
    return {"image": imageio.imread(gcs.open(path), **kwargs),
            "name": path.split("/")[-1],
            "output": output
           }

def save_tile(el):
    # gcs = gcsio.GcsIO()
    tier = el["tier"]
    tier_number = el["tier_number"]
    item = el["item"]
    tile_group = el["tile_group"]
    output = el["output"] + el["name"]
    path = f"{output}/TileGroup{tile_group}/{tier_number}-{item[1]}-{item[0]}.jpg"

    f = io.BytesIO()
    # gcs.open(path, 'w', mime_type="image/jpeg")
    try:
        imageio.imwrite(
            #f"image/TileGroup-{next(tile_group)}-{tier_number}-{item[0]}-{item[1]}.jpg",
            f,
            tier[item[0]*256:min(tier.shape[0],item[0]*256+256), item[1]*256:min(tier.shape[1],item[1]*256+256), :3],
            format="JPG", quality=80)
        return [(path, f.getvalue())]
    except ValueError as e:
        print(e)

def main(argv=None, save_main_session=True):
  """Main entry point"""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://YOUR_INPUT_BUCKET/AND_INPUT_PREFIX/',
      help='Input folder to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
      help='Output file to write results to.')
  parser.add_argument(
      '--extensions',
      dest='extensions',
      default='jpg',
      help='File extensions to be processed.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  valid_extensions = known_args.extensions.split(',')

  with beam.Pipeline(options=pipeline_options) as p:
    # Read the text file[pattern] into a PCollection.
    files = p | MatchFiles(known_args.input)
    (
        files
        | 'Filter by file extension' >> beam.Filter(by_extension, valid_extensions)
        | 'Read images to arrays' >> beam.Map(lambda x: img_read(x.path, known_args.output))
        | 'Tile images' >> beam.ParDo(ProcessTiles())
        | 'Save tile' >> beam.ParDo(save_tile)
        | 'Upload to GCS' >> beam.ParDo(UploadGCS())
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
