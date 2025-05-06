FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flight_checker.py .

CMD ["python", "flight_checker.py"]
