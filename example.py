#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from mldata.datasets import Datasets
from mldata.interpreters.bin_image import BinImage

__author__ = 'Iván de Paz Centeno'


token = "1b57b8a85bdc403b8d20eefd91a9f8e1"

def remove_elements(dataset):
    for id in dataset.keys(): del dataset[id]


datasets = Datasets(token)

dataset = datasets[0]
print(dataset)
