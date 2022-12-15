FROM python:3.11-slim-buster
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN pip install asgiref==3.5.2 && \
pip install Django==4.1.3 && \
pip install django-cors-headers==3.13.0 && \
pip install djangorestframework==3.14.0 && \
pip install pytz==2022.1 && \
pip install pytzdata==2020.1 && \
pip install sqlparse==0.4.3 && \
pip install psycopg2-binary
COPY . .