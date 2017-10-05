#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from time import sleep

from dhub.datasets import Datasets

__author__ = "Iván de Paz Centeno"
#sleep(5)
d = Datasets()

#d.add_dataset("example", "none", "pep", "ah", ["asd"])
print(d)

del d[0]

#element = dataset.add_element("aa", "bb", ["asdd"], "nasd", b"hello")
#print(element.get_content())
#print(dataset[0])
#print(dataset[0].get_content())
sleep(4)