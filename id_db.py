# -*- coding: utf-8 -*-

from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, BigInteger, Text, TIMESTAMP, text, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from id_common import get_child, to_bool

import datetime

Base = declarative_base()


# сайты
class Site(Base):
	__tablename__ = 'site'

	site_id = Column(Integer, nullable=False, primary_key=True)	# Ид
	name = Column(Text)											# сервер
	niche_id = Column(Integer)									# Ид ниши
	time_load = Column(TIMESTAMP, server_default=text('NOW()')) # время загрузки
	user_load = Column(Text)									# пользователь загрузки
	feed = Column(Text)
	filename = Column(Text) 									# НаименованиеФайла


# табличка с данными по продукту
class FeedStore(Base):
	__tablename__ = 'feed_store'

	available = Column(Boolean)									# Доступность
	id = Column(Integer, nullable=False, primary_key=True)		# Внешний идентификатор
	code = Column(Text)											# Код
	name = Column(Text)											# Наименование
	url = Column(Text)											# Url
	price = Column(Numeric) 									# Цена
	price_old = Column(Numeric)
	currency = Column(Text)										# валюта
	img_small =	Column(Text)									# картинка малая
	img_large = Column(Text)  									# картинка_увеличенная
	site = Column(Text)  										# сайт наименование
	time_load = Column(TIMESTAMP, server_default=text('NOW()')) # время загрузки
	time_xml = Column(BigInteger)								# время xml(UTC)
	path_img_small = Column(Text)  								# путь к малой картинке в файловой системе
	path_img_large = Column(Text)  								# путь к большой картинке в файловой системе
	user_load = Column(Text)


# табличка с данными по размерам продукта
class FeedProdStore(Base):
	__tablename__ = 'feed_prod_store'

	available = Column(Boolean)									# Доступность
	id = Column(Integer, nullable=False, primary_key=True)		# Внешний идентификатор
	code = Column(Text)											# Код
	name = Column(Text)											# Наименование
	url = Column(Text)											# Url
	price = Column(Numeric) 									# Цена
	price_old = Column(Numeric)
	site = Column(Text)  										# сайт наименование
	time_load = Column(TIMESTAMP, server_default=text('NOW()')) # время загрузки
	time_xml = Column(BigInteger)  # время xml(UTC)
	user_load = Column(Text)
	param_name = Column(Text)									# info from "params" xml tag, name of the product subtype
	param_available = Column(Boolean)							# is the subtype available?
	param_price = Column(Numeric)								# its price
	param_price_old = Column(Numeric)							# its old price


engine = None
DBSession = None
session = None


def connect(db_username, db_password, db_host, db_name):
	global engine
	global DBSession
	global session

	engine = create_engine('postgresql://{}:{}@{}/{}'.format(db_username, db_password, db_host, db_name))

	Base.metadata.bind = engine

	DBSession = sessionmaker(bind=engine)

	session = DBSession()


def create_db():
	Base.metadata.create_all(engine)


def get_sites():
	return session.query(Site).all()


def store_product_data(site_name, product, xml_timestamp, small_img_path, large_img_path):
	code = get_child(product, "code")
	db_prod = session.query(FeedStore).filter_by(code=code).first()

	if not db_prod:
		db_prod = FeedStore(available=to_bool(get_child(product, "avalible")),
							code=code,
							name=get_child(product, "name"),
							url=get_child(product, "url"),
							price=int(get_child(product, "price")),
							price_old=int(get_child(product, "price_old")),
							currency=get_child(product, "currency"),
							img_small=get_child(product, "img_small"),
							img_large=get_child(product, "img_large"),
							site=site_name,
							time_xml=xml_timestamp,
							path_img_small=small_img_path,
							path_img_large=large_img_path)
		session.add(db_prod)
		session.commit()
	else:
		db_prod.available = to_bool(get_child(product, "avalible"))
		db_prod.code = code
		db_prod.name = get_child(product, "name")
		db_prod.url = get_child(product, "url")
		db_prod.price = int(get_child(product, "price"))
		db_prod.price_old = int(get_child(product, "price_old"))
		db_prod.currency = get_child(product, "currency")
		db_prod.img_small = get_child(product, "img_small")
		db_prod.img_large = get_child(product, "img_large")
		db_prod.site = site_name
		db_prod.time_xml = xml_timestamp
		db_prod.time_load = datetime.datetime.now()
		db_prod.path_img_small = small_img_path
		db_prod.path_img_large = large_img_path
		session.commit()


def store_product_sizes(site_name, product_info, xml_timestamp):
	code = get_child(product_info, "code")

	params = product_info.find("params")
	if params:
		for param in params:
			param_name = param.get("name")
			param_available = param.get("avalible")
			param_price = param.get("price")
			try:
				if not param_price or param_price=="0":
					param_price = int(get_child(product_info, "price"))
				else:
					param_price = int(param_price)
			except ValueError as e:
				param_price = 0

			if param_available:
				param_available = to_bool(param_available)

			param_price_old = param.get("price_old")
			try:
				if not param_price_old or param_price_old=="0":
					param_price_old = int(get_child(product_info, "price_old"))
				else:
					param_price_old = int(param_price_old)
			except ValueError as e:
				param_price_old = 0

			db_prod_size_entry = session.query(FeedProdStore)\
									.filter_by(code=code, param_name=param_name)\
									.first()

			if not db_prod_size_entry:
				db_prod_size_entry = FeedProdStore(available=to_bool(get_child(product_info, "avalible")),
										code=code,
										name=get_child(product_info, "name"),
										url=get_child(product_info, "url"),
										price=int(get_child(product_info, "price")),
										price_old=int(get_child(product_info, "price_old")),
										site=site_name,
										time_xml=xml_timestamp,
										param_name=param_name,
										param_available=param_available,
										param_price=param_price,
										param_price_old=param_price_old)

				session.add(db_prod_size_entry)
				session.commit()
			else:
				db_prod_size_entry.available=to_bool(get_child(product_info, "avalible"))
				db_prod_size_entry.code=code
				db_prod_size_entry.name=get_child(product_info, "name")
				db_prod_size_entry.url = get_child(product_info, "url")
				db_prod_size_entry.price=int(get_child(product_info, "price"))
				db_prod_size_entry.price_old=int(get_child(product_info, "price_old"))
				db_prod_size_entry.site=site_name
				db_prod_size_entry.time_xml=xml_timestamp
				db_prod_size_entry.time_load=datetime.datetime.now()
				db_prod_size_entry.param_name=param_name
				db_prod_size_entry.param_available=param_available
				db_prod_size_entry.param_price=param_price
				db_prod_size_entry.param_price_old=param_price_old
				session.commit()
	else:
		param_name = None
		param_available = to_bool(get_child(product_info, "avalible"))
		param_price = int(get_child(product_info, "price"))
		param_price_old = int(get_child(product_info, "price_old"))

		db_prod_size_entry = session.query(FeedProdStore) \
			.filter_by(code=code, param_name=param_name) \
			.first()

		if not db_prod_size_entry:
			db_prod_size_entry = FeedProdStore(available=to_bool(get_child(product_info, "avalible")),
											   code=code,
											   name=get_child(product_info, "name"),
											   url = get_child(product_info, "url"),
											   price=int(get_child(product_info, "price")),
											   price_old=int(get_child(product_info, "price_old")),
											   site=site_name,
											   time_xml=xml_timestamp,
											   param_name=param_name,
											   param_available=param_available,
											   param_price=param_price,
											   param_price_old=param_price_old)

			session.add(db_prod_size_entry)
			session.commit()
		else:
			db_prod_size_entry.available = to_bool(get_child(product_info, "avalible"))
			db_prod_size_entry.code = code
			db_prod_size_entry.name = get_child(product_info, "name")
			db_prod_size_entry.url = get_child(product_info, "url")
			db_prod_size_entry.price = int(get_child(product_info, "price"))
			db_prod_size_entry.price_old = int(get_child(product_info, "price_old"))
			db_prod_size_entry.site = site_name
			db_prod_size_entry.time_xml = xml_timestamp
			db_prod_size_entry.time_load = datetime.datetime.now()
			db_prod_size_entry.param_name = param_name
			db_prod_size_entry.param_available = param_available
			db_prod_size_entry.param_price = param_price
			db_prod_size_entry.param_price_old = param_price_old
			session.commit()

