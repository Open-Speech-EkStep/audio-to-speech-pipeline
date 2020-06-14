FROM python:3.7

RUN apt-get update && apt-get upgrade -y

RUN mkdir /opt/data_cataloguer/
WORKDIR /opt/data_cataloguer/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/data_cataloguer/