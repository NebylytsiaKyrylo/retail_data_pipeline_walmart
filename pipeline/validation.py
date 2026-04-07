import logging
import os

from config import DATA_PATHS

logger = logging.getLogger(__name__)


def validation() -> bool:
    """
    Validates the existence of specific files.

    This function checks whether the required files specified in the given
    paths exist in the file system. If any of the files are missing, an
    error is logged, and the validation process fails. If all files are
    present, the function logs their existence and returns a successful
    validation.

    Returns:
        bool: True if all required files exist, otherwise False.
    """
    for file in [DATA_PATHS['clean_output'], DATA_PATHS['agg_output']]:
        if not os.path.exists(file):
            logger.error('An error occurred while validating files')
            return False
        logger.info(f'{file} exists')
    return True
