FROM ubuntu:18.04

## System packages
# I'm used to using `apt` but it complains that it doesn't have a stable
#   command-line interface, so let's use `apt-get` instead
RUN apt-get update \
 && apt-get -y install \
    libigraph0-dev \
    libxml2-dev \
    python3 \
    python3-dev \
    python3-pip \
    zlib1g-dev \
 && rm -rf /var/cache/apt/*

## Python packages
COPY requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt \
 && rm -rf /root/.cache/pip

COPY alevin_to_anndata.py /opt
COPY scanpy_entry_point.py /opt
WORKDIR /opt
