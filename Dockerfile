FROM python:2.7-slim
MAINTAINER Thomas Maschler thomas.maschler@wri.org

RUN apt-get -y update && apt-get -y install git
RUN pip install git+https://github.com/wri/hadoop_pip
