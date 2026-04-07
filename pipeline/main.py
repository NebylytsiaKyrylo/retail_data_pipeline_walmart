import logging

from config import DATA_PATHS
from pipeline.extract import get_db_engine, extract
from pipeline.transform import transform
from pipeline.load import load
from pipeline.aggregate import aggregate
from pipeline.validation import validation

# Initialize logging
logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("Starting Walmart Data Pipeline")
    logger.info("=" * 60)

    db_engine = get_db_engine()
    extracted_df = extract(db_engine, DATA_PATHS['parquet_input'])
    transformed_df = transform(extracted_df)
    agg_data = aggregate(transformed_df)
    load(transformed_df, agg_data)

    if validation():
        logger.info("Pipeline completed successfully!")
    else:
        logger.error("Pipeline completed with validation errors")


if __name__ == "__main__":
    main()
