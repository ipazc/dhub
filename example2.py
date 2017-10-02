#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldata.datasets import Datasets
from mldata.interpreters.bin_image import BinImage

__author__ = 'Iván de Paz Centeno'

token = "1b57b8a85bdc403b8d20eefd91a9f8e1"

datasets = Datasets(token)


print(datasets)

fg_net = datasets['ipazc/fg-net']
print(fg_net)

fg_net.set_binary_interpreter(BinImage())

print(fg_net[0])
fg_net[0].get_content().show()

fg_net.save_to_folder("fg_net", elements_extension=".jpg", use_numbered_ids=True)