#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dhub
# Copyright (C) 2017 Iván de Paz Centeno <ipazc@unileon.es>.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

from io import BytesIO
from PIL import Image
from dhub.interpreters.interpreter import Interpreter

__author__ = 'Iván de Paz Centeno'

class BinImage(Interpreter):

    def cipher(self, image_bytes):
        with BytesIO(image_bytes) as bytes_io, Image.open(bytes_io) as im:
            image = im.convert("RGB")

        return image

    def decipher(self, pil_image):
        with BytesIO() as bytes_io:
            pil_image.save(bytes_io, "PNG")
            bytes_io.seek(0)
            result = bytes_io.read()

        return result
