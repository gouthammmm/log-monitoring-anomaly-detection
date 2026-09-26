from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.routes import logs, anomalies

app = FastAPI(title="Log Monitoring & Anomaly Detection API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(logs.router)
app.include_router(anomalies.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}


# Serves the dashboard's static files (built/copied into /app/static)
app.mount("/", StaticFiles(directory="static", html=True), name="dashboard")
