import json
import pandas as pd
import pytest
from pipelines.transform_gold import flatten_companyfacts

def test_flatten_companyfacts_smoke():
    """
    A smoke test for the flatten_companyfacts function.
    """
    # 1. Load the test fixture
    fixture_path = "tests/fixtures/sample_aapl_facts.json"
    with open(fixture_path, 'r') as f:
        sample_facts = json.load(f)

    # 2. Call the function under test
    df = flatten_companyfacts(sample_facts, "AAPL")

    # 3. Assertions
    assert isinstance(df, pd.DataFrame)

    # Assert on columns
    expected_columns = [
        "symbol", "cik", "taxonomy", "concept", "unit", "period_end",
        "fy", "fp", "form", "filed", "value", "accn", "source_url"
    ]
    assert set(df.columns) == set(expected_columns)

    # Assert on row count (fixture has 3 unique facts)
    assert len(df) == 3

    # Assert on source_url format
    assert df['source_url'].str.startswith('https://www.sec.gov/Archives/edgar/data/').all()

    # Assert on data integrity
    assert df['symbol'].eq('AAPL').all()
    assert df['cik'].eq(320193).all()
    assert df['concept'].isin(['Revenues', 'OperatingIncomeLoss']).all()

    # Check one value to be sure
    revenue_val = df[df['concept'] == 'Revenues']['value'].iloc[0]
    assert revenue_val == 383285000000
