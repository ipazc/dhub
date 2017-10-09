#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# DHub
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


import sys
from setuptools import setup, setuptools
from dhub import __version__

__author__ = 'Iván de Paz Centeno'


def readme():
    with open('README.rst') as f:
        return f.read()

if sys.version_info < (3, 4, 1):
    sys.exit('Python < 3.4.1 is not supported!')

setup(name='dhub',
      version=__version__,
      description='ML Data Hub client for managing datasets of MlDataHub from Python3.',
      long_description=readme(),
      url='http://github.com/ipazc/dhub',
      author='Iván de Paz Centeno',
      author_email='ipazc@unileon.es',
      license='GNU GPLv3 or later',
      packages=setuptools.find_packages(),
      install_requires=[
          "pyzip",
          "pyfolder",
          "requests",
          "pillow"
      ],
      classifiers=[
          'Development Status :: 1 - Planning',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: Education',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Framework :: Flask',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)'
      ],
      #entry_points = {
      #    'console_scripts': ['dhub=dhub.entry_point:main'],
      #},
      data_files = [
          ('/etc/dhub', ['dhub/config.json'])
      ],
      test_suite='nose.collector',
      tests_require=['nose'],
      include_package_data=True,
      keywords="machine-learning mldatahub data hub datasets dataset store client",
      zip_safe=False)
