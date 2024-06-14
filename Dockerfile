FROM ubuntu:22.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y parallel git python3 python-is-python3 python3-dev python3-pip python3-venv

RUN python -m venv /venv
RUN . /venv/bin/activate
RUN pip install -U pip
COPY requirements.txt /reqs.txt
RUN pip install -r /reqs.txt
ENV LC_ALL C.UTF-8
