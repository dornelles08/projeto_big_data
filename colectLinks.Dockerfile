FROM python:3.10

WORKDIR /app

RUN mkdir /app/output

COPY orm.py /app/orm.py
COPY mongo.py /app/mongo.py

COPY colectLinks.py /app/colectLinks.py

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["python", "/app/colectLinks.py"]