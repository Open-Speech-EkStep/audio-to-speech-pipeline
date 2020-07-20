FROM python:3.7

RUN mkdir /opt/downloaded_data_cataloguer/
WORKDIR /opt/downloaded_data_cataloguer/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/downloaded_data_cataloguer/