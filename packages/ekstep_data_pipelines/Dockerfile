FROM python:3.7

RUN mkdir /opt/ekstep_data_pipelines/
WORKDIR /opt/ekstep_data_pipelines/


COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /opt/ekstep_data_pipelines/