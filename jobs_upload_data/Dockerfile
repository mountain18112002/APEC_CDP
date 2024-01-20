FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod 777 /app/check_point/status.txt

CMD ["python", "automatically_upload_data.py"]