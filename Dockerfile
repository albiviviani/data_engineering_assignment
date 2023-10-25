FROM python:3.11

COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /
COPY . /

CMD ["python", "etl_app/main.py"]
