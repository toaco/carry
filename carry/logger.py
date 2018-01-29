from __future__ import unicode_literals

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(message)s',
    filename='carry.log',
    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(message)s')
console.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(console)
