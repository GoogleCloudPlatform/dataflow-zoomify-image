# Image Zoomifier

This repository contains a Dataflow template that can be used to tile images from Cloud Storage.

## How to use this template

1. Clone it from GitHub.
1. Edit the `export_variables.sh` file inside the `template` folder with your variables
1. The service account used in GOOGLE_APPLICATION_CREDENTIALS needs the right permissions (Dataflow and Cloud Storage)
1. Enter the `template` folder and run `venv_prepare.sh` to prepare the environment
1. Run `run_batch.sh` to start the Dataflow pipeline.
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
