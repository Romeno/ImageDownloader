# -*- coding: utf-8 -*-

import logging
import os
import os.path
import time

import requests
from lxml import etree

import id_db
from id_common import get_child

program_name = "ImageDownloader"
crawl_delay = 0.15


def init_logger():
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
				'filename': 'errors.log',
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


def get_products(site):
	logger = logging.getLogger()

	try:
		resp = requests.get("http://{}/feedxml_crm.php".format(site.name), verify=False)
		if resp.ok:
			root = etree.fromstring(resp.content)
			if len(root) == 0:
				logger.error("Empty xml of product list for some reason for site {}".format(site.name))
				return None, None

			return root[0], root.get("timestamp")
		else:
			logger.error("Error {} when getting {} products".format(resp.status_code, site.name))
			return None, None
	except requests.RequestException as e:
		logger.exception("Requests exception when getting {} products".format(site.name))
		return None, None


def get_product_info(site, product):
	logger = logging.getLogger()

	code = get_child(product, "code")
	if code is None:
		return None

	logger.info("Getting product {} info ".format(code))

	try:
		time.sleep(crawl_delay)
		resp = requests.get("http://{}/feedxml_crm.php?code={}".format(site.name, code), verify=False)
		if resp.ok:
			root = etree.fromstring(resp.content)
			if len(root) == 0 or len(root[0]) == 0:
				logger.info("Empty xml for site {}".format(site.name))
				return None

			return root[0][0]
		else:
			logger.error("Error {} when getting {} product {}".format(resp.status_code, site.name, code))
			return None
	except requests.RequestException as e:
		logger.exception("Requests exception when getting {} product {}".format(site.name, code))
		return None


def download_images(site, product, base_path):
	logger = logging.getLogger()
	paths = None

	code = get_child(product, "code")
	img_small = get_child(product, "img_small")
	img_large = get_child(product, "img_large")

	try:
		path_small = ""
		path_large = ""

		if img_small:
			time.sleep(crawl_delay)
			resp = requests.get("http://{}/{}".format(site.name, img_small), verify=False)

			if resp.ok:
				if not img_small.startswith("/"):
					path_small = base_path + "/" + site.name + "/" + img_small
				else:
					path_small = base_path + "/" + site.name + img_small

				os.makedirs(os.path.dirname(path_small), exist_ok=True)
				with open(path_small, "wb") as f:
					f.write(resp.content)
			else:
				logger.error("Error {} when downloading images for {} of {}".format(resp.status_code, code, site.name))

		if img_large:
			time.sleep(crawl_delay)
			resp = requests.get("http://{}/{}".format(site.name, img_large), verify=False)

			if resp.ok:
				if not img_large.startswith("/"):
					path_large = base_path + "/" + site.name + "/" + img_large
				else:
					path_large = base_path + "/" + site.name + img_large

				os.makedirs(os.path.dirname(path_large), exist_ok=True)
				with open(path_large, "wb") as f:
					f.write(resp.content)
			else:
				logger.error("Error {} when downloading images for {} of {}".format(resp.status_code, code, site.name))

		paths = [path_small, path_large]
	except requests.RequestException as e:
		logger.warning("Images were not downloaded due to network error")
		logger.exception("Requests exception when downloading images for {} of {}".format(code, site.name))

	return paths


def main():
	from id_config import db_username, db_password, db_host, db_name, base_path

	init_logger()

	logger = logging.getLogger()

	try:
		id_db.connect(db_username, db_password, db_host, db_name)

		for site in id_db.get_sites():
			logger.info("Started site {}".format(site.name))

			try:
				products, xml_timestamp = get_products(site)
				if products is None:
					logger.error("Skipping site {} due to error while getting product list".format(site.name))
					continue

				for product in products:
					paths = download_images(site, product, base_path)
					if paths:
						id_db.store_product_data(site, product, xml_timestamp, *paths)
					else:
						logger.warning("Images were not downloaded for product code {}, site {}".format(get_child(product, "code"), site.name))

					product_info = get_product_info(site, product)
					if product_info:
						id_db.store_product_sizes(site, product_info, xml_timestamp)
					else:
						logger.info("Cannot get product sizes for product code {}, site {}".format(get_child(product, "code"), site.name))

				logger.info("Finished site {}".format(site.name))
			except Exception as e:
				logger.exception("Exception during {} run".format(program_name))
				logger.error("Skipping site {}".format(site.name))

	except Exception as e:
		logger.exception("Exception during {} run".format(program_name))
		raise

	logger.info("Finished!!!")


if __name__ == "__main__":
	main()