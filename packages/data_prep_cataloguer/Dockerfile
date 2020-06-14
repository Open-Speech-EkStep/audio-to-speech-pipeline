FROM python:3.7

RUN mkdir /opt/data_prep_cataloguer/
WORKDIR /opt/data_prep_cataloguer/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/data_prep_cataloguer/