#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldata.datasets import Datasets
from mldata.interpreters.bin_image import BinImage

__author__ = 'Iván de Paz Centeno'


token = "260d59be41e14ed2ac28484380314178"




datasets = Datasets(token)
dataset = datasets["ipazc/example"]

dataset.set_binary_interpreter(BinImage())

#image = Image.open("/home/ivan/example1.jpg")

#element = dataset.add_element("image3", "example for image 3", ["example"], "none", "/home/ivan/example1.jpg")

#print(element)

#image = element.get_content()
#image.show()

print(len(dataset))
print(dataset.keys())

for element in dataset:
    content = element.get_content()
    if content is not None:
        content.show()


