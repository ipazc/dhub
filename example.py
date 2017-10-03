#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from io import BytesIO
import os
from time import sleep
from zipfile import ZipFile, ZIP_DEFLATED
from mldata.datasets import Datasets
from mldata.interpreters.bin_image import BinImage

__author__ = 'Iván de Paz Centeno'


token = "1b57b8a85bdc403b8d20eefd91a9f8e1"

def remove_elements(dataset):
    for id in dataset.keys(): del dataset[id]

datasets = Datasets(token)
dataset = datasets[0]

#dataset[0].set_ref("ref1")
#dataset[1].set_ref("ref2")
dataset[2].set_content(b"holba2")
sleep(5)
dataset[2].set_ref("jebd")
print(dataset[2])
dataset.close()

"""iteration = -1
for element in dataset:
    iteration += 1
    content = element.get_content()
    if content == b"a":
        print(element)
        print(dataset[5])
        print("{}: {}".format(iteration, content))
        break
"""


#elements = dataset[0:5]

#for element in elements:
#    print(element)
#    print(element.get_content())
"""dataset = datasets[0]

element = dataset[0]

content = element.get_content()

with BytesIO() as b:
    with ZipFile(b, mode="w", compression=ZIP_DEFLATED) as z:
        z.writestr("data1", content)
        z.writestr("data2", content)
    b.seek(0)
    zipped_content = b.read()

print(len(zipped_content))

with BytesIO(zipped_content) as b, ZipFile(b) as z:
    result = z.read("data2")

bin_image = BinImage()
bin_image.cipher(result).show()
"""