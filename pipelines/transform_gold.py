import pandas as pd

def flatten_companyfacts(facts_json, symbol):
    """
    Transforms raw company facts JSON for all us-gaap concepts into a flattened DataFrame.

    Args:
        facts_json (dict): The raw JSON data from the SEC companyfacts API.
        symbol (str): The stock ticker symbol.

    Returns:
        pd.DataFrame: A DataFrame with the flattened data for all numeric us-gaap concepts.
    """
    required_columns = [
        "symbol", "cik", "taxonomy", "concept",
        "unit", "period_end", "fy", "fp", "form",
        "filed", "value", "accn", "source_url"
    ]

    gaap_facts = facts_json.get("facts", {}).get("us-gaap", {})
    if not gaap_facts:
        return pd.DataFrame(columns=required_columns)

    cik = facts_json.get("cik")
    all_rows = []

    # Iterate over each concept in us-gaap facts
    for concept_name, concept_data in gaap_facts.items():
        # Iterate over each unit for the concept (e.g., USD, shares)
        for unit_name, facts_list in concept_data.get("units", {}).items():
            for fact in facts_list:
                # Some facts are just descriptions, skip if no value
                if 'val' not in fact:
                    continue

                accn_no_dashes = fact.get("accn", "").replace("-", "")
                source_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                    f"{accn_no_dashes}/{fact.get('accn')}.txt"
                )

                all_rows.append({
                    "symbol": symbol,
                    "cik": cik,
                    "taxonomy": "us-gaap",
                    "concept": concept_name,
                    "unit": unit_name,
                    "period_end": fact.get("end"),
                    "fy": fact.get("fy"),
                    "fp": fact.get("fp"),
                    "form": fact.get("form"),
                    "filed": fact.get("filed"),
                    "value": fact.get("val"),
                    "accn": fact.get("accn"),
                    "source_url": source_url,
                })

    if not all_rows:
        return pd.DataFrame(columns=required_columns)

    df = pd.DataFrame(all_rows)

    # De-duplicate the data, keeping the latest filing for each fact
    df['filed'] = pd.to_datetime(df['filed'])
    df = df.sort_values(by='filed', ascending=False)
    df = df.drop_duplicates(
        subset=['symbol', 'concept', 'unit', 'period_end'],
        keep='first'
    )

    return df
