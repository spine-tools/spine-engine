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
The function of this class can be used for generation of keys for enabling security 
of the remote spine_server. The code has been copied based on an example at (by Chris Laws):
https://github.com/zeromq/pyzmq/blob/main/examples/security/generate_certificates.py

"""

import os
import shutil
import sys
import zmq.auth


class CertificateCreator:
    @staticmethod
    def generate_certificates(base_dir):
        """Generates client and server keys for enabling security.

        Args:
            base_dir: folder where the files are created.
        """
        keys_dir = os.path.join(base_dir, "certificates")
        public_keys_dir = os.path.join(base_dir, "public_keys")
        secret_keys_dir = os.path.join(base_dir, "private_keys")

        # Create directories for certificates, remove old content if necessary
        for d in [base_dir, keys_dir, public_keys_dir, secret_keys_dir]:
            if os.path.exists(d):
                shutil.rmtree(d)
            os.mkdir(d)

        # create new keys in certificates dir
        server_public_file, server_secret_file = zmq.auth.create_certificates(keys_dir, "server")
        client_public_file, client_secret_file = zmq.auth.create_certificates(keys_dir, "client")
        # move public keys to appropriate directory
        for key_file in os.listdir(keys_dir):
            if key_file.endswith(".key"):
                shutil.move(os.path.join(keys_dir, key_file), os.path.join(public_keys_dir, "."))
        # move secret keys to appropriate directory
        for key_file in os.listdir(keys_dir):
            if key_file.endswith(".key_secret"):
                shutil.move(os.path.join(keys_dir, key_file), os.path.join(secret_keys_dir, "."))


def main(args):
    if len(args) < 2:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.join(script_dir, "certs")
        cert_dir = os.path.join(base_dir, "certificates")
        if os.path.exists(base_dir):
            print(f"Directory {base_dir} already exists. Please remove it to recreate certificates.")
            return 0
    else:
        print("Too many arguments")
        return 0
    try:
        CertificateCreator.generate_certificates(base_dir)
    except Exception as e:
        print(f"Creating certificates failed. Error:{e}")
        return 1
    print(f"Certificates successfully created to: {base_dir}")
    return 0


if __name__ == "__main__":
    main(sys.argv)
