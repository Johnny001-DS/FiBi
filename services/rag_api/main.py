from fastapi import FastAPI

app = FastAPI()

@app.get("/healthz")
def health_check():
    """
    Health check endpoint to verify that the service is running.
    """
    return {"status": "ok"}
