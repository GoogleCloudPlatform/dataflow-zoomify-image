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

export PROJECT=your-project
export REGION=us-central1
export BUCKET=your-bucket
export INPUT_FOLDER=your/input/folder
export OUTPUT_FOLDER=your/output/folder
export NUM_WORKERS=10
export STAGING_FOLDER=staging
export TEMP_FOLDER=tmp
export GOOGLE_APPLICATION_CREDENTIALS=/your/key/path.json