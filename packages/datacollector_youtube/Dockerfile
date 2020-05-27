FROM python:3.7

RUN mkdir /opt/datacollector-youtube/
WORKDIR /opt/datacollector-youtube/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/datacollector-youtube/