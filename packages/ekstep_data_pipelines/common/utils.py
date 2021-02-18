import sys
import logging


def get_logger(name):
    """
    Function for returning the script logger
    """
    logger = logging.getLogger(__name__ + name)
    logger.setLevel(logging.DEBUG)

    logging_format = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    logging_handler = logging.StreamHandler(sys.stdout)
    logging_handler.setFormatter(logging_format)

    logger.addHandler(logging_handler)

    return logger
