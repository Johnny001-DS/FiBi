import os
import pandas as pd
import logging

_fundamentals_cache = None

def load_fundamentals(data_path="data/gold/fact_fundamentals_quarterly"):
    """
    Loads the partitioned Parquet dataset of fundamental facts.

    This function caches the data in memory after the first load to avoid
    repeated disk I/O.

    Args:
        data_path (str): The path to the root of the partitioned Parquet dataset.

    Returns:
        pd.DataFrame: The loaded DataFrame of fundamental facts, or an empty
                      DataFrame if the data cannot be loaded.
    """
    global _fundamentals_cache

    if _fundamentals_cache is not None:
        return _fundamentals_cache

    if not os.path.exists(data_path):
        logging.warning(f"Data path not found: {data_path}. Returning empty DataFrame.")
        # Cache an empty df to prevent re-trying on every call
        _fundamentals_cache = pd.DataFrame(columns=['symbol'])
        return _fundamentals_cache

    try:
        logging.info(f"Loading fundamental data from {data_path} into cache...")
        df = pd.read_parquet(data_path, engine='pyarrow')
        _fundamentals_cache = df
        logging.info("Successfully loaded fundamental data into cache.")
        return _fundamentals_cache
    except Exception as e:
        logging.error(f"Failed to load fundamental data: {e}")
        _fundamentals_cache = pd.DataFrame(columns=['symbol'])
        return _fundamentals_cache
