#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from time import sleep

from mldata.datasets import Datasets

__author__ = 'Iván de Paz Centeno'

datasets = Datasets()
"""element_kwargs = [
    {'title':'a', 'description': 'a', 'tags': ['age: 5'], 'http_ref': "none", 'content': "hola{}".format(x).encode()} for x in range(50)
] + [
    {'title':'b', 'description': 'b', 'tags': ['age: 15'], 'http_ref': "none", 'content': "holb{}".format(x).encode()} for x in range(20)
]

elements = dataset.add_elements(element_kwargs)
"""
dataset = datasets[1]
print(datasets)
for dataset in datasets:
    print(dataset)

#new_dataset = dataset.fork("lala", "my title", "my desc", ["tag 1", "tag 2"], "none", datasets)
#print(new_dataset)
exit(0)

#fork = dataset.fork("test3", "notitle", "nodesc", ["hi", "a"], "none", datasets, {'tags':{'$regex':'age[:][ ]?5'}})

#print(fork)
print(dataset)

for element in dataset:
    print(element)
exit(0)
#dataset.add_element("Added_juts_now", "now added", ["age: 1"], "no ref", b"mycontent")

"""for element in dataset.filter_iter({'tags':{'$regex':'age[:][ ]?5'}}):
    print(element)
print(dataset)
sleep(2)
"""
#dataset.clear()
for element in dataset:
    print(element)
"""
element = dataset[0]
print(element.get_content())
#element.set_title("titlte33")
element.set_content(b"hi234")
print(element.get_content())
print(dataset[0].get_content())
"""

#dataset.close()
#element = dataset.add_element("title1", "desc1", ["hi", "ho"], "none", b"hola")
#dataset.close()