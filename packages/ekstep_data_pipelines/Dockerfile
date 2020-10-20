FROM python:3.7

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y ffmpeg
RUN apt-get install -y sox
RUN apt-get install -y gcc-multilib g++-multilib
RUN apt-get install libpq-dev
RUN apt-get install libsndfile-dev


RUN mkdir /opt/ekstep_data_pipelines/
WORKDIR /opt/ekstep_data_pipelines/


COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/ekstep_data_pipelines/