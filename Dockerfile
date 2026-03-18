FROM mcr.microsoft.com/devcontainers/python:3.11

COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip
RUN pip3 install --requirement /tmp/requirements.txt
