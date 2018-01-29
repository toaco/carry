from __future__ import unicode_literals

from carry.logger import logger


class MockProgressbar(object):
    def __init__(self, desc):
        logger.info(desc)

    def update(self, num):
        pass
