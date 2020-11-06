FROM python:3.8.6

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y ffmpeg
RUN apt-get install -y sox
RUN apt-get install -y gcc-multilib g++-multilib
RUN apt-get install -y libpq-dev
RUN apt-get install -y libsndfile1

RUN mkdir /opt/ekstep_data_pipelines/
WORKDIR /opt/ekstep_data_pipelines/


COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ekstep_data_pipelines /opt/ekstep_data_pipelines/
COPY invocation_script.py /opt/invocation_script.py