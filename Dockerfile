FROM python:3.11-slim
WORKDIR /app
COPY aggregation_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY aggregation_service/*.py ./
ENV PYTHONUNBUFFERED=1
EXPOSE 8001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
