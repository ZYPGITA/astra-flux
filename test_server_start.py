# -*- coding: utf-8 -*-

import os

from astraflux import *

from servers.test_server import test_server, sub_test_server

current_dir = os.path.dirname(__file__)

AstraFlux(yaml_path=f'{current_dir}/config.yaml', current_dir=current_dir)

launch_register(services=[
    test_server,
    sub_test_server
])
launch_start()

if __name__ == '__main__':
    while True:
        pass
