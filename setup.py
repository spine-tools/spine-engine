######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Setup script for Python's setuptools.

:authors: M. Marin (KTH)
:date:   20.11.2019
"""

from setuptools import setup, find_packages

REQUIRED_SPINEDB_API_VERSION = "0.10.2"

with open("README.md", encoding="utf8") as readme_file:
    readme = readme_file.read()

version = {}
with open("spine_engine/version.py") as fp:
    exec(fp.read(), version)

install_requires = [
    "dagster >= 0.9.15, <= 0.10.1",
    "sqlalchemy >= 1.3",
    "numpy >= 1.15.1",
    "openpyxl > 3.0",
    "gdx2py >= 2.0.4",
    "ijson >= 2.6.1",
    "spinedb_api >= {}".format(REQUIRED_SPINEDB_API_VERSION),
]


setup(
    name="spine_engine",
    version=version["__version__"],
    description="A package to run Spine workflows.",
    long_description=readme,
    author="Spine Project consortium",
    author_email="spine_info@vtt.fi",
    url="https://github.com/Spine-project/spine-engine",
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    license="LGPL-3.0-or-later",
    zip_safe=False,
    keywords="",
    classifiers=[],
    install_requires=install_requires,
    test_suite="tests",
)
