
import logging
import sys

from src.ui_actions import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(message)s')
formatter.default_msec_format = '%s.%03d'
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
fh = logging.FileHandler(__name__+".log")
fh.setFormatter(formatter)
logger.addHandler(fh)




if __name__ == "__main__":
    #from_csv_transform_pandas_to_csv()
    from_csv_transform_dask_to_csv()