#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldata.datasets import Datasets

__author__ = 'Iván de Paz Centeno'


datasets = Datasets("260d59be41e14ed2ac28484380314178")

print(datasets["ipazc/example"])

#del datasets["ipazc/example2"]

dataset = datasets["ipazc/example"]
element = dataset.add_element("hello", "hi", ["hello"], "none", b"")
print(element)

print(len(dataset))
#print(datasets)

#dataset = datasets.add_dataset("example", "this is an example of dataset", ["tag1", "tag2"], "example", "no reference")

#print(dataset)