FROM python:3.7

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y ffmpeg
RUN apt-get install -y sox
RUN apt-get install -y gcc-multilib g++-multilib

RUN mkdir /opt/dataprocessor/
WORKDIR /opt/dataprocessor/

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/dataprocessor/