#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from time import sleep

from mldata.datasets import Datasets

__author__ = 'Iván de Paz Centeno'


token = "edcc5b9cd94840559267f9c9f454d9b1"

datasets = Datasets(token)

"""
element_kwargs = [
    {'title':'a', 'description': 'a', 'tags': ['age: 5'], 'http_ref': "none", 'content': "hola{}".format(x).encode()} for x in range(50)
] + [
    {'title':'b', 'description': 'b', 'tags': ['age: 15'], 'http_ref': "none", 'content': "holb{}".format(x).encode()} for x in range(20)
]
"""

dataset = datasets[0]
#elements = dataset.add_elements(element_kwargs)

"""for element in dataset.filter_iter({'tags':{'$regex':'age[:][ ]?5'}}):
    print(element)
print(dataset)
sleep(2)
"""

element = dataset[0]
print(element.get_content())
#element.set_title("titlte33")
element.set_content(b"hi234")
print(element.get_content())
print(dataset[0].get_content())


#dataset.close()
#element = dataset.add_element("title1", "desc1", ["hi", "ho"], "none", b"hola")
#dataset.close()