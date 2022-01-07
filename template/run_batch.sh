#! /bin/bash
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

source venv/bin/activate

source export_variables.sh

python batch_tile_images.py --input gs://$INPUT_BUCKET$INPUT_FOLDER/** \
 --output gs://$OUTPUT_BUCKET$OUTPUT_FOLDER/ \
 --bigquery-project=$BIGQUERY_PROJECT \
 --bigquery-dataset=$BIGQUERY_DATASET \
 --bigquery-table=$BIGQUERY_TABLE \
 --final-bucket=$FINAL_BUCKET \
 --extensions=$FILE_EXTENSIONS \
 --requirements_file requirements.txt \
 --runner=DataflowRunner \
 --autoscaling_algorithm=NONE \
 --num_workers=$NUM_WORKERS \
 --project=$PROJECT \
 --region=$REGION \
 --staging_location=gs://$STAGING_BUCKET/$STAGING_FOLDER \
 --temp_location=gs://$TEMP_BUCKET/$TEMP_FOLDER \
 --job_name=$JOB_NAME
