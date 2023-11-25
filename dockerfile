FROM python:latest


RUN apt-get update && apt-get install -y curl

WORKDIR /app

COPY pipeline.py pipeline.py
COPY download_file.sh /app/download_file.sh

RUN chmod +x /app/download_file.sh
RUN pip install pandas numpy sqlalchemy psycopg2

CMD ["python", "pipeline.py"]

