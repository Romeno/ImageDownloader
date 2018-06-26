# -*- coding: utf-8 -*-

from lxml import etree
import logging


def get_child(element, tag_name):
    logger = logging.getLogger()

    try:
        el = element.find(tag_name)
        if el is None:
            return None
        else:
            return el.text
    except etree.LxmlError as e:
        logger.warning("Cannot get {} from xml".format(tag_name))
        logger.exception("Error getting '{}' from xml".format(tag_name))
        return None


def to_bool(text):
    return True if text == "true" else False


def init_logger(filename):
    from logging.config import dictConfig

    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'f': {
                'format':
                    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
            }
        },
        'handlers': {
            'h': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'f',
                'level': 'DEBUG',
                'filename': filename,
                'maxBytes': 10485760,
                'backupCount': 20,
                'encoding': 'utf8'
            }
        },
        'root': {
            'handlers': ['h'],
            'level': logging.INFO,
        },
    }

    dictConfig(logging_config)