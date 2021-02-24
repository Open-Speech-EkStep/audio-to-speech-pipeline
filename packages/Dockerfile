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
COPY ekstep_data_pipelines /opt/ekstep_data_pipelines/ekstep_data_pipelines
COPY invocation_script.py /opt/ekstep_data_pipelines

COPY lib/wada_snr.tar.gz .
RUN tar -xzvf wada_snr.tar.gz
RUN mkdir -p /opt/ekstep_data_pipelines/ekstep_data_pipelines/binaries/WadaSNR
RUN mv WadaSNR /opt/ekstep_data_pipelines/ekstep_data_pipelines/binaries/WadaSNR
RUN ls /opt/ekstep_data_pipelines/ekstep_data_pipelines/binaries/
RUN rm wada_snr.tar.gz