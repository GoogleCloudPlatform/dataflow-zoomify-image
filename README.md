# Image Zoomifier

This repository contains a Dataflow template that can be used to tile images from Google Cloud Storage according to the Zoomify specification.

## How to use this template

1. Clone it from GitHub.
2. Create a BigQuery project/dataset/table with the following fields: barcode (str), imagecode (str), extension (str), path (str), width (int), height (int), md5 (str), modified (timestamp).
3. Edit the `export_variables.sh` file inside the `template` folder with your variables, creating the buckets if necessary.
4. The service account used in GOOGLE_APPLICATION_CREDENTIALS needs the right permissions (Dataflow, Cloud Storage and BigQuery).
5. Upload sample images to your INPUT_BUCKET.
6. Enter the `template` folder and run `venv_prepare.sh` to prepare the environment.
7. Run `run_batch.sh` to start the Dataflow pipeline.

## How it works

1. First, original images must be placed in the INPUT_FOLDER of your INPUT_BUCKET.
2. The script will scan all images there with the specified FILE_EXTENSIONS.
3. For each image, the script will extract its image code (file name without the extension) and check if it already exists in the BigQuery table.
4. If it exists and if the file MD5 matches the corresponding MD5 from the record, then we are dealing with an image that was already processed and is unchanged, so the original image is simply discarded and erased from the bucket.
5. If it does not exist (new image) or if the MD5 is different (changed image), the script will generate the tiles and save them in the OUTPUT_FOLDER of the OUTPUT_BUCKET inside a directory named as the image code and under the same path of the original image. If an image is updated, the existing tiles are first erased.
6. After processing an image, the original file is moved to the FINAL_BUCKET under the same path.

Therefore, an image such as:

    gs://input-bucket/mypath/test.jpg

has tiles generated in:

    gs://output-bucket/mypath/test/TileGroup0/
    ...
    gs://output-bucket/mypath/test/TileGroupN/

and is moved to:

    gs://final-bucket/mypath/test.jpg

Image viewers, like OpenSeaDragon, can then be configured to display the zoomified image, loading different sets of tiles at different zoom levels.

##


    Copyright 2021 Google LLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
