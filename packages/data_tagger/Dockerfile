FROM python:3.7

RUN apt-get update && apt-get upgrade -y

RUN mkdir /opt/data_tagger/
WORKDIR /opt/data_tagger/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/data_tagger/