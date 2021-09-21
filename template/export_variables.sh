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

PROJECT=your-project
REGION=us-central1
BUCKET=your-bucket
INPUT_FOLDER=your/input/folder
OUTPUT_FOLDER=your/output/folder
NUM_WORKERS=10
STAGING_FOLDER=staging
TEMP_FOLDER=tmp
GOOGLE_APPLICATION_CREDENTIALS=/your/key/path.json
JOB_NAME=job-name