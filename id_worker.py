#!/usr/bin/python3.5
# -*- coding: utf-8 -*-

import logging
import os
import os.path
import time

import requests
from lxml import etree

import id_db
from id_common import get_child, init_logger
from id_config import program_name, base_worker_logger_name, crawl_delay


class ImageDownloader:
	def __init__(self, site_name, base_path):
		self.site_name = site_name
		self.base_path = base_path
		self.worker_logger_name = "{}{}.log".format(base_worker_logger_name, self.site_name)

	def run(self):
		from id_config import db_username, db_password, db_host, db_name

		init_logger(self.worker_logger_name)

		logger = logging.getLogger(self.worker_logger_name)

		logger.info("Started site {}".format(self.site_name))

		try:
			id_db.connect(db_username, db_password, db_host, db_name)

			products, xml_timestamp = self.get_products(self.site_name)
			if products is None:
				logger.error("Skipping site {} due to error while getting product list".format(self.site_name))
				return

			processed_codes = id_db.get_product_codes_for_site(self.site_name)
			processed_codes = [c[0] for c in processed_codes]
			skip_count = 0

			for product in products:
				code = get_child(product, "code")
				if code is None:
					continue

				if code in processed_codes:
					skip_count += 1
					continue

				paths = self.download_images(self.site_name, product, self.base_path)
				if paths:
					id_db.store_product_data(self.site_name, product, xml_timestamp, *paths)
				else:
					logger.warning(
						"Images were not downloaded for product code {}, site {}".format(get_child(product, "code"),
																						 self.site_name))

				product_info = self.get_product_info(self.site_name, product)
				if product_info:
					id_db.store_product_sizes(self.site_name, product_info, xml_timestamp)
				else:
					logger.info(
						"Cannot get product sizes for product code {}, site {}".format(get_child(product, "code"),
																					   self.site_name))

			logger.info("Skipped {} products as they where already in DB".format(skip_count))
			logger.info("Finished site {}".format(self.site_name))
		except Exception as e:
			logger.exception("Exception during {} run".format(program_name))
			logger.error("Skipping site {}".format(self.site_name))
		finally:
			id_db.disconnect()

	def get_products(self, site_name):
		logger = logging.getLogger(self.worker_logger_name)

		try:
			resp = requests.get("http://{}/feedxml_crm.php".format(site_name), verify=False)
			if resp.ok:
				root = etree.fromstring(resp.content)
				if len(root) == 0:
					logger.error("Empty xml of product list for some reason for site {}".format(site_name))
					return None, None

				return root[0], root.get("timestamp")
			else:
				logger.error("Error {} when getting {} products".format(resp.status_code, site_name))
				return None, None
		except requests.RequestException as e:
			logger.exception("Requests exception when getting {} products".format(site_name))
			return None, None

	def get_product_info(self, site_name, product):
		logger = logging.getLogger(self.worker_logger_name)

		code = get_child(product, "code")
		if code is None:
			return None

		logger.info("Getting product {} info ".format(code))

		try:
			time.sleep(crawl_delay)
			resp = requests.get("http://{}/feedxml_crm.php?code='{}'".format(site_name, code), verify=False)
			if resp.ok:
				root = etree.fromstring(resp.content)
				if len(root) == 0 or len(root[0]) == 0:
					logger.info("Empty xml for site {}".format(site_name))
					return None

				return root[0][0]
			else:
				logger.error("Error {} when getting {} product {}".format(resp.status_code, site_name, code))
				return None
		except requests.RequestException as e:
			logger.exception("Requests exception when getting {} product {}".format(site_name, code))
			return None

	def download_images(self, site_name, product, base_path):
		logger = logging.getLogger(self.worker_logger_name)
		paths = None

		code = get_child(product, "code")
		img_small = get_child(product, "img_small")
		img_large = get_child(product, "img_large")

		try:
			path_small = ""
			path_large = ""

			if img_small:
				time.sleep(crawl_delay)
				resp = requests.get("http://{}/{}".format(site_name, img_small), verify=False)

				if resp.ok:
					if not img_small.startswith("/"):
						path_small = base_path + "/" + site_name + "/" + img_small
					else:
						path_small = base_path + "/" + site_name + img_small

					os.makedirs(os.path.dirname(path_small), exist_ok=True)
					with open(path_small, "wb") as f:
						f.write(resp.content)
				else:
					logger.error("Error {} when downloading images for {} of {}".format(resp.status_code, code, site_name))

			if img_large:
				time.sleep(crawl_delay)
				resp = requests.get("http://{}/{}".format(site_name, img_large), verify=False)

				if resp.ok:
					if not img_large.startswith("/"):
						path_large = base_path + "/" + site_name + "/" + img_large
					else:
						path_large = base_path + "/" + site_name + img_large

					os.makedirs(os.path.dirname(path_large), exist_ok=True)
					with open(path_large, "wb") as f:
						f.write(resp.content)
				else:
					logger.error("Error {} when downloading images for {} of {}".format(resp.status_code, code, site_name))

			paths = [path_small, path_large]
		except requests.RequestException as e:
			logger.warning("Images were not downloaded due to network error")
			logger.exception("Requests exception when downloading images for {} of {}".format(code, site_name))

		return paths