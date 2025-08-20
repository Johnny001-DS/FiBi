from fastapi import FastAPI
from .fundamentals import load_fundamentals

app = FastAPI()

@app.get("/healthz")
def health_check():
    """
    Health check endpoint to verify that the service is running.
    """
    return {"status": "ok"}

@app.get("/fundamentals/{symbol}")
def get_fundamentals(symbol: str):
    """
    API endpoint to retrieve fundamental data for a given stock symbol.

    (Note: This is a skeleton implementation.)
    """
    df = load_fundamentals()

    # For the skeleton, we just check if the symbol exists and return a static shape.
    # The actual data processing will be added later.
    if not df.empty and symbol.upper() in df['symbol'].unique():
        # Symbol is found in our data
        pass # No special logic for now
    else:
        # Symbol is not found
        pass # No special logic for now

    return {
        "symbol": symbol.upper(),
        "as_of": None,
        "metrics": {}
    }
