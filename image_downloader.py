#!/usr/bin/python3.5
import logging
from utils.process import SilentProcessPool

from id_config import program_name

import id_db
import id_worker
import id_common


def start_downloader_instance(q):
    site, base_path = q
    try:
        image_downloader = id_worker.ImageDownloader(site, base_path)
        image_downloader.run()
    except KeyboardInterrupt:
        raise
    except:
        pass

# end of StartCrawler


def main():
    from id_config import db_username, db_password, db_host, db_name, base_path, process_pool_size, runner_log_name

    id_common.init_logger(runner_log_name)

    logger = logging.getLogger(runner_log_name)

    try:
        id_db.connect(db_username, db_password, db_host, db_name)

        sites = id_db.get_sites()
        sites = [site.name for site in sites]

        id_db.disconnect()

        pp = SilentProcessPool(poolLength=process_pool_size, worker=start_downloader_instance,
                               data=zip(sites, [base_path] * len(sites)))
        pp.logger_name = runner_log_name
        pp.Run()

        logger.info("Finished!!!")

    except Exception as e:
        logger.exception("Exception during {} run".format(program_name))
        raise

# end of main


if __name__ == "__main__":
    main()













