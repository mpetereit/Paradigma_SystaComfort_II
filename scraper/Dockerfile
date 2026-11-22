FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY paradigma_scraper.py .

# Speicherort fuer die persistierte Browser-Session (storage_state.json),
# als Volume mounten damit nicht bei jedem Neustart neu eingeloggt wird.
VOLUME ["/data"]

CMD ["python", "paradigma_scraper.py"]
