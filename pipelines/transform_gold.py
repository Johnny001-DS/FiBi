import pandas as pd

def flatten_companyfacts(facts_json, symbol):
    """
    Transforms raw company facts JSON for a specific concept into a flattened DataFrame.

    Args:
        facts_json (dict): The raw JSON data from the SEC companyfacts API.
        symbol (str): The stock ticker symbol.

    Returns:
        pd.DataFrame: A DataFrame with the flattened data for the 'Revenues' concept,
                      or an empty DataFrame if the concept is not found.
    """
    # Define the columns for the output DataFrame
    required_columns = [
        "symbol", "cik", "taxonomy", "concept",
        "unit", "period_end", "fy", "fp", "form",
        "filed", "value", "accn", "source_url"
    ]

    # Use .get() to safely access nested keys
    gaap_facts = facts_json.get("facts", {}).get("us-gaap", {})
    revenues_concept = gaap_facts.get("Revenues")

    if not revenues_concept:
        return pd.DataFrame(columns=required_columns)

    # Prefer 'USD' unit, but fall back to the first available unit
    units = revenues_concept.get("units", {})
    if "USD" in units:
        unit_key = "USD"
    elif units:
        unit_key = list(units.keys())[0]
    else:
        return pd.DataFrame(columns=required_columns)

    facts_list = units.get(unit_key, [])
    if not facts_list:
        return pd.DataFrame(columns=required_columns)

    cik = facts_json.get("cik")
    rows = []
    for fact in facts_list:
        accn_no_dashes = fact.get("accn", "").replace("-", "")
        source_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/"
            f"{accn_no_dashes}/{fact.get('accn')}.txt"
        )

        rows.append({
            "symbol": symbol,
            "cik": cik,
            "taxonomy": "us-gaap",
            "concept": "Revenues",
            "unit": unit_key,
            "period_end": fact.get("end"),
            "fy": fact.get("fy"),
            "fp": fact.get("fp"),
            "form": fact.get("form"),
            "filed": fact.get("filed"),
            "value": fact.get("val"),
            "accn": fact.get("accn"),
            "source_url": source_url,
        })

    if not rows:
        return pd.DataFrame(columns=required_columns)

    return pd.DataFrame(rows)
