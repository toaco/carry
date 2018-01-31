from __future__ import unicode_literals

import os
import sys

import carry


def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = 'carfile.py'

    if not os.path.exists(config_path):
        print("Can't find the config file {}".format(config_path))
    else:
        carry.run(config_path)
