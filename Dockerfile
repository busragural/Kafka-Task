FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
COPY scraper.py scraper.py
COPY api.py api.py

RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh", "-c", "python /app/scraper.py & python /app/api.py"]