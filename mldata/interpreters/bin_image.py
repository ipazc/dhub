#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from io import BytesIO
from PIL.Image import Image

__author__ = 'Iván de Paz Centeno'

class BINImage(object):

    def __init__(self):
        self.binary = binary

    def interpret(self, image_bytes):
        with BytesIO(image_bytes) as bytes_io, Image.open(bytes_io) as im:
            image = im.convert("RGB")

        return image

    def deinterpret(self, pil_image):
        with BytesIO() as bytes_io:
            pil_image.save(bytes_io, "PNG")
            bytes_io.seek(0)
            result = bytes_io.read()

        return result
