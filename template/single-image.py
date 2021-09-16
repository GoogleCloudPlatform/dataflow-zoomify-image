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

from io import BytesIO
from os import X_OK
import time
import math
import itertools
from PIL import Image
import imageio

tile_size = 256

tiers = []
images = []

t1 = time.time()
im = imageio.imread("/Users/egon/Downloads/cria_images/images/tests/CLF/TESTE01.jpg")
t2 = time.time()
tier = math.ceil(max([math.log(shape/tile_size, 2) for shape in im.shape]))
max_step = 2 ** tier
steps = [int(max_step/2**i) for i in range(tier)]
tiers = [im[::step, ::step] for step in steps]
print(tier, len(tiers))
t3 = time.time()
def next_i():
     i = 0
     while i < 256:
         yield i // 256
         i += 1
tile_group = next_i()
for tier_number, tier in enumerate(tiers):
    for item in itertools.product([i for i in range(math.ceil(tier.shape[0]/256))], [i for i in range(math.ceil(tier.shape[1]/256))]):
        f = BytesIO()
        imageio.imwrite(
            #f"image/TileGroup-{next(tile_group)}-{tier_number}-{item[0]}-{item[1]}.jpg",
            f,
            tier[item[0]*256:min(tier.shape[0],item[0]*256+256),
            item[1]*256:min(tier.shape[1],item[1]*256+256)],
            format="JPG")
# tile-column-row.jpeg
#imageio.imwrite(f"image{tier}.jpg", im[:256,:256])
t4 = time.time()
print(t2-t1, t3-t2, t4 - t3)