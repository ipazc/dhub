#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldata.datasets import Datasets

__author__ = 'Iván de Paz Centeno'


datasets = Datasets("260d59be41e14ed2ac28484380314178")
dataset = datasets["ipazc/example"]
print(len(dataset))

#dataset["59ca8973b9a7c05218470839"].set_content(b"hello!")
ids = [element.get_id() for element in dataset]
print(ids)

for id in ids:
    print(id)
    del dataset[id]
    print("deleted")

print(dataset.get_url_prefix())
print(dataset.keys())

print(len(dataset))

dataset.keys()
#print(datasets)

#dataset = datasets.add_dataset("example", "this is an example of dataset", ["tag1", "tag2"], "example", "no reference")

#print(dataset)