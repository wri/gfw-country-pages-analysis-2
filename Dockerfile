FROM python:2.7-slim
MAINTAINER Thomas Maschler thomas.maschler@wri.org

ENV SRC_PATH /usr/src/gfw

RUN mkdir $SRC_PATH

RUN apt-get -y update && apt-get -y install git
RUN pip install --upgrade pip

COPY setup.py $SRC_PATH
COPY gfw_country_pages_analysis_2 $SRC_PATH/gfw_country_pages_analysis_2
COPY config $SRC_PATH/config

RUN cd $SRC_PATH && pip install .