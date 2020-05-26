FROM python:3.7

RUN mkdir /opt/dataprocessor/
WORKDIR /opt/dataprocessor/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/dataprocessor/