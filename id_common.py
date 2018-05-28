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

