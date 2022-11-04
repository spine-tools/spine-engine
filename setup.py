######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
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

with open("README.md", encoding="utf8") as readme_file:
    readme = readme_file.read()

version = {}
with open("spine_engine/version.py") as fp:
    exec(fp.read(), version)

install_requires = [
    "dagster>=0.12.6, <0.12.9",  # dagster >= 0.12.9 requires alembic that is incompatible with spinedb_api
    "protobuf<3.21.0",  # https://developers.google.com/protocol-buffers/docs/news/2022-05-06#python-updates
    "networkx>2.5.1",
    "datapackage>=1.15.2, <1.16",
    "jupyter_client<7.0",
    "spinedb_api>=0.18.0",
    "pyzmq >=21.0",
    "markupsafe < 2.1",  # dagster 0.12.8 requires Jinja2<3.0, which tries to import soft_unicode, which has been removed in markupsafe 2.1
]

setup(
    name="spine_engine",
    version=version["__version__"],
    description="A package to run Spine workflows.",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Spine Project consortium",
    author_email="spine_info@vtt.fi",
    url="https://github.com/Spine-project/spine-engine",
    packages=find_packages(exclude=("tests", "tests.*")),
    package_data={'spine_engine': ['execution_managers/spine_repl.jl']},
    license="LGPL-3.0-or-later",
    zip_safe=False,
    keywords="",
    install_requires=install_requires,
    test_suite="tests",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/Spine-project/spine-engine/issues",
        #"Documentation": ""
    },
)
