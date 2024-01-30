######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Engine contributors
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains static methods for handling ZIP files.
"""

import os
import shutil
from zipfile import ZipFile
from spine_engine.utils.helpers import get_file_size


class ZipHandler:
    """ZIP file handler."""

    @staticmethod
    def package(src_folder, dst_folder, fname):
        """Packages a directory into a ZIP-file.

        NOTE: Do not use the src_folder as the dst_folder. If the ZIP file is
        created to the same directory as root_dir, there will be a corrupted
        project_package.zip file INSIDE the actual project_package.zip file, which
        makes unzipping the file fail.

        Args:
            src_folder (str): Folder to be included to the ZIP file
            dst_folder (str): Destination folder for the ZIP file
            fname (str): Name of the ZIP-file without extension (it's added by shutil.make_archive())
        """
        zip_path = os.path.join(dst_folder, fname)
        try:
            shutil.make_archive(zip_path, "zip", src_folder)
        except OSError:
            raise

    @staticmethod
    def extract(zip_file, output_folder):
        """Extracts the contents of a ZIP file to the provided folder.

        Args:
            zip_file (str): Absolute path to ZIP file to be extracted.
            output_folder (str): Absolute path to destination directory
        """
        if not os.path.exists(zip_file):
            raise ValueError(f"ZIP file '{zip_file}' does not exist")
        file_size = os.path.getsize(zip_file)
        if file_size < 100:
            raise ValueError(f"'{zip_file}' possibly corrupted. File size too small [{get_file_size(file_size)}]")
        with ZipFile(zip_file, "r") as zip_obj:
            try:
                first_bad_file = zip_obj.testzip()  # Test ZIP file integrity before extraction (debugging)
                if not first_bad_file:
                    zip_obj.extractall(output_folder)
                else:
                    print(f"'{zip_file}' integrity test failure. First bad file: {first_bad_file}")
            except Exception as e:
                raise e

    @staticmethod
    def delete_folder(folder):
        """Removes the provided directory and all it's contents.

        Args:
            folder: Directory to be removed
        """
        if not folder:
            raise ValueError("Invalid input. No folder given.")
        if not os.path.isdir(folder):
            raise ValueError(f"Given dir:{folder} does not exist.")
        try:
            shutil.rmtree(folder)
        except OSError:
            raise
