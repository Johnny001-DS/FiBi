import os
import requests
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of tickers to fetch data for
TICKERS = ["AAPL", "MSFT", "SPY"]

# SEC URLs
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_API_ENDPOINT_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Your email address for the User-Agent header
USER_AGENT = "RAG Financial Analysis Agent info@example.com"

# Directory to store the downloaded data
DATA_DIR = "data/sec"

def get_cik_map():
    """
    Downloads the company tickers from the SEC and returns a CIK map.
    """
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(COMPANY_TICKERS_URL, headers=headers)
    response.raise_for_status()
    all_tickers = response.json()
    # The JSON file is a dictionary where keys are indices and values are company info
    # We want to create a map of ticker -> CIK
    cik_map = {item['ticker']: str(item['cik_str']).zfill(10) for key, item in all_tickers.items()}
    return cik_map

def fetch_company_facts():
    """
    Fetches company facts data from the SEC EDGAR API for the specified tickers.
    """
    # Ensure the data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    try:
        logging.info("Fetching CIK map...")
        cik_map = get_cik_map()
        logging.info("CIK map fetched successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CIK map: {e}")
        return

    # Get the API key from the environment variable
    api_key = os.getenv("SEC_API_KEY")

    # Set up the headers for the request
    headers = {"User-Agent": USER_AGENT}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    for ticker in TICKERS:
        cik = cik_map.get(ticker)
        if not cik:
            logging.warning(f"Could not find CIK for ticker: {ticker}")
            continue

        logging.info(f"Fetching data for {ticker} (CIK: {cik})...")

        url = SEC_API_ENDPOINT_TEMPLATE.format(cik=cik)

        try:
            # Make the request to the SEC API
            response = requests.get(url, headers=headers)

            # Handle cases where the ticker exists but has no data (e.g., some ETFs)
            if response.status_code == 404:
                logging.warning(f"No data found for {ticker} (CIK: {cik}) at companyfacts endpoint. It may be an ETF.")
                continue

            response.raise_for_status()  # Raise an exception for other bad status codes

            data = response.json()

            # Check for the presence of us-gaap facts
            if "us-gaap" not in data.get("facts", {}):
                logging.warning(f"No us-gaap facts found for {ticker}. Skipping file save.")
                continue

            # Save the data to a JSON file
            output_path = os.path.join(DATA_DIR, f"{ticker}.json")
            with open(output_path, "w") as f:
                json.dump(data, f, indent=4)

            logging.info(f"Successfully saved data for {ticker} to {output_path}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {ticker}: {e}")

        # Respect the SEC's rate limit (10 requests per second)
        time.sleep(0.1)

if __name__ == "__main__":
    fetch_company_facts()
