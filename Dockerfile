FROM python:latest

WORKDIR /app

RUN pip install pandas

COPY pipeline.py pipeline_c.py
RUN python pipeline_c.py https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv target.csv

ENTRYPOINT [ "bash" ]