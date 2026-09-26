FROM python:3.11-slim
WORKDIR /app
COPY api-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api-gateway/src/ ./src/
COPY dashboard/src/ ./static/
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
