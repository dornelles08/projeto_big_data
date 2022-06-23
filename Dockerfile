FROM python:3.10

WORKDIR /app

RUN mkdir /app/output

COPY orm.py /app/orm.py
COPY mongo.py /app/mongo.py
COPY testConection.py /app/testConection.py

COPY dataProcessing.py /app/dataProcessing.py

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["python", "/app/dataProcessing.py"]