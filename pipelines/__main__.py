import argparse
import json
import os
import logging
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
        default="data/gold",
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

    for ticker, raw_facts in all_raw_data.items():
        output_path = os.path.join(args.out, f"tmp_revenues_{ticker}.parquet")

        logging.info(f"Processing {ticker}...")

        df = flatten_companyfacts(raw_facts, ticker)

        if not df.empty:
            logging.info(f"Found {len(df)} rows of revenue data for {ticker}.")
            df.to_parquet(output_path, index=False)
            logging.info(f"Successfully wrote Parquet file to {output_path}")
        else:
            logging.warning(f"No revenue data found for {ticker}. No Parquet file will be written.")

if __name__ == "__main__":
    main()
