FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    "fastapi>=0.115,<1" \
    "uvicorn[standard]>=0.30,<1" \
    "pydantic>=2,<3" \
    "httpx>=0.27,<1" \
    "confluent-kafka>=2,<3"
COPY services services
COPY data data
COPY scripts scripts
ENV PO_DATA_DIR=/app/data
EXPOSE 8000
CMD ["uvicorn", "services.source_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
