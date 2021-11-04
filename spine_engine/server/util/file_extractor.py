######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Contains FileExtractor class for extraction of ZIP files in Spine Engine.
:authors: P. Pääkkönen (VTT), P. Savolainen (VTT)
:date:   23.08.2021
"""

from zipfile import ZipFile
import shutil
import os


class FileExtractor:
    """ZIP-file extractor in Spine Engine."""

    @staticmethod
    def extract(zipFile, outputFolder):
        """Extracts the content of a ZIP-file to the provided folder.

        Args:
            zipFile (str): Absolute path to ZIP-file to be extracted.
            outputFolder (str): Absolute path to folder where the contents are extracted to
        """
        if not zipFile or not outputFolder:
            raise ValueError('invalid input to FileExtractor.extract()')
        zipfile_exists = os.path.exists(zipFile)
        fileSize = os.path.getsize(zipFile)
        if not zipfile_exists:
            raise ValueError(f"zipfile '{zipFile}'does not exist")
        if fileSize < 100:
            raise ValueError(f"zipfile '{zipFile}' is too small. File size:{fileSize}")
        with ZipFile(zipFile, "r") as zipObj:
            try:
                zipObj.extractall(outputFolder)
            except Exception as e:
                raise e

    @staticmethod
    def deleteFolder(folder):
        """Deletes the provided folder and all contents of it.
        
        Args:
            folder: folder to be deleted
        """
        # check input
        if folder == None:
            raise ValueError('invalid input to FileExtractor.deleteFolder()')
        if len(folder) == 0:
            raise ValueError('invalid input to FileExtractor.deleteFolder()')
        if os.path.isdir(folder) == False:
            raise ValueError('provided folder %s doesn' 't exist' % folder)
        shutil.rmtree(folder)
