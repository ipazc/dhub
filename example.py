#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from time import sleep
from mldata.datasets import Datasets
from mldata.interpreters.bin_image import BinImage

__author__ = 'Iván de Paz Centeno'


token = "b6fb00069ec5421b83cfe2a949ffebf6"

def remove_elements(dataset):
    elements_ids = [element.get_id() for element in dataset]
    for id in elements_ids: del dataset[id]


datasets = Datasets(token)
dataset = datasets[0]

print(len(dataset))

#remove_elements(dataset)
"""files = [element for element in os.listdir("/home/ivan/") if element.endswith(".jpg")]

for filename in files:
    dataset.add_element("unknown", "unknown", ["none"], "none", os.path.join("/home/ivan/", filename))

print("added {} elements in dataset".format(len(dataset)))
"""

print(len(dataset))
#dataset.set_binary_interpreter(BinImage())

dataset.save_to_folder("dataset/", metadata_format="json", elements_extension=".jpg")

#dataset.set_binary_interpreter(BinImage())

#image = Image.open("/home/ivan/example1.jpg")

#element = dataset.add_element("image3", "example for image 3", ["example"], "none", "/home/ivan/example1.jpg")

#print(element)

#image = element.get_content()
#image.show()
"""
print(len(dataset))
print(dataset.keys())

for element in dataset:
    content = element.get_content()
    if content is not None:
        content.show()


"""