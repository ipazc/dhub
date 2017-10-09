#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dhub.interpreters.bin_image import BinImage
from dhub.datasets import Datasets

__author__ = 'Iván de Paz Centeno'

datasets = Datasets()

#fg = datasets["ipazc/fg-net"]

#del datasets["ipazc/fg-net2"]
print(datasets)

#fg2 = datasets["fg-net2"]

#fg2.set_binary_interpreter(BinImage())
#fg2[1000].get_content().show()
#print(fg2[133], )
#fg = datasets['fg-net']
#fg.save_to_folder("fg-net", elements_extension=".jpg", use_numbered_ids=True)
#fg2.load_from_folder("fg-net")

