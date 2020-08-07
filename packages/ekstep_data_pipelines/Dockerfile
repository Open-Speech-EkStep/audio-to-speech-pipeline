FROM python:3.7

RUN mkdir /opt/data_marker/
WORKDIR /opt/data_marker/


COPY requirements.txt
RUN pip install -r requirements.txt
COPY . /opt/data_marker/