FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd --create-home appuser && mkdir -p /app/data /app/logs && chown -R appuser:appuser /app
USER appuser
CMD ["python", "main.py"]
