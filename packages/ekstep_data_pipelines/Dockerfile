FROM python:3.7

RUN mkdir /opt/ekstep_data_pipelines/
WORKDIR /opt/ekstep_data_pipelines/


COPY ekstep_requirements.txt .
RUN pip install -r ekstep_requirements.txt
COPY . /opt/ekstep_data_pipelines/