FROM python:3.11-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install psycopg2-binary
COPY . .
#CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]