FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY models/ models/
COPY artifacts/ artifacts/
COPY src/ src/

ENV PYTHONUNBUFFERED=1
CMD ["python", "src/service.py"]
