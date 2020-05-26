FROM python:3.7

RUN apt-get update
RUN apt-get install ffmpeg
RUN apt-get install sox
RUN apt-get install gcc-multilib g++-multilib

RUN mkdir /opt/dataprocessor/
WORKDIR /opt/dataprocessor/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/dataprocessor/