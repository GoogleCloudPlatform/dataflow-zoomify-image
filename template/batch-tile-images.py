"""A workflow for tiling images
"""

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.gcp import gcsio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


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
      # CHANGE 1/6: The Google Cloud Storage path is required
      # for outputting the results.
      default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DataflowRunner',
      # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--project=egon-ongcp-demos',
      # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
      # is required in order to run your pipeline on the Google Cloud
      # Dataflow Service.
      '--region=us-central1',
      # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://egon-ongcp-demos-lcm/cria-staging',
      # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://egon-ongcp-demos-lcm/cria-tmp',
      '--job_name=cria-job',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:
    def img_read(path):
        import imageio
        gcs = gcsio.GcsIO()
        return imageio.imread(gcs.open(path))

    def process_tiles(im, output):
        import math
        import itertools
        import imageio
        tier = math.ceil(max([math.log(shape/256, 2) for shape in im.shape]))
        max_step = 2 ** tier
        steps = [int(max_step/2**i) for i in range(tier)]
        tiers = [im[::step, ::step] for step in steps]
        def next_i():
            i = 0
            while i < 256:
                yield i // 256
                i += 1
        tile_group = next_i()
        gcs = gcsio.GcsIO()
        for tier_number, tier in enumerate(tiers):
            for item in itertools.product([i for i in range(math.ceil(tier.shape[0]/256))], [i for i in range(math.ceil(tier.shape[1]/256))]):
                f = gcs.open(f"{output}/image/TileGroup-{next(tile_group)}-{tier_number}-{item[0]}-{item[1]}.jpg", 'w', mime_type="image/jpeg")
                imageio.imwrite(
                    #f"image/TileGroup-{next(tile_group)}-{tier_number}-{item[0]}-{item[1]}.jpg",
                    f,
                    tier[item[0]*256:min(tier.shape[0],item[0]*256+256),
                    item[1]*256:min(tier.shape[1],item[1]*256+256)],
                    format="JPG")

    # Read the text file[pattern] into a PCollection.
    files = p | MatchFiles(known_args.input)
    objects = (
        files
        | 'Read images to arrays' >> beam.Map(lambda x: img_read(x.path))
        | 'Tile images and save to gcs' >> beam.Map(lambda x: process_tiles(x, known_args.output))
    )



    # # Count the occurrences of each word.
    # counts = (
    #     lines
    #     | 'Split' >> (
    #         beam.FlatMap(
    #             lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str))
    #     | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
    #     | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # # Format the counts into a PCollection of strings.
    # def format_result(word_count):
    #   (word, count) = word_count
    #   return '%s: %s' % (word, count)

    # output = counts | 'Format' >> beam.Map(format_result)

    # # Write the output using a "Write" transform that has side effects.
    # # pylint: disable=expression-not-assigned
    # output | WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()