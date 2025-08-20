import argparse
import json
import os
import logging
import pandas as pd
from pipelines.transform_gold import flatten_companyfacts
from ingest.sec.sec_ingest import fetch_company_facts

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Main function to run the data transformation pipeline.
    """
    parser = argparse.ArgumentParser(description="Data processing pipeline.")

    parser.add_argument(
        "--out",
        type=str,
        default="data/gold/fact_fundamentals_quarterly",
        help="Output directory for the processed data."
    )

    args = parser.parse_args()

    tickers_to_process = ["AAPL", "MSFT"]

    # Ensure the output directory exists
    os.makedirs(args.out, exist_ok=True)

    logging.info(f"Starting data ingestion for {tickers_to_process}...")
    all_raw_data = fetch_company_facts(tickers_to_fetch=tickers_to_process)

    if not all_raw_data:
        logging.error("Ingestion failed to retrieve any data. Aborting.")
        return

    logging.info("Ingestion complete. Starting transformation...")

    all_dfs = []
    for ticker, raw_facts in all_raw_data.items():
        logging.info(f"Processing {ticker}...")
        df = flatten_companyfacts(raw_facts, ticker)
        if not df.empty:
            logging.info(f"Found {len(df)} rows of de-duplicated data for {ticker}.")
            all_dfs.append(df)
        else:
            logging.warning(f"No data found for {ticker}.")

    if not all_dfs:
        logging.error("No dataframes were created. Aborting.")
        return

    # Combine all dataframes and write to a partitioned parquet dataset
    logging.info("Combining dataframes...")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    logging.info(f"Total rows in combined dataframe: {len(combined_df)}")

    try:
        logging.info(f"Writing partitioned dataset to {args.out}...")
        combined_df.to_parquet(
            args.out,
            engine='pyarrow',
            partition_cols=['symbol']
        )
        logging.info("Successfully wrote partitioned dataset.")
    except Exception as e:
        logging.error(f"Failed to write partitioned dataset: {e}")


if __name__ == "__main__":
    main()
