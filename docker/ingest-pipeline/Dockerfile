# Parent image
FROM python:3.7-slim-buster

LABEL description="HuBMAP Ingest Pipeline" \
	version="0.2"

ENV DEBIAN_FRONTEND noninteractive

ARG AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}
ARG AIRFLOW_UID="50000"
ARG AIRFLOW_GID="50000"
ENV AIRFLOW_UID=${AIRFLOW_UID}
ENV AIRFLOW_GID=${AIRFLOW_GID}
ARG AIRFLOW_USER_HOME_DIR=/home/airflow
ENV AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR}

WORKDIR /opt/airflow

# need git to install other hubmap requirements
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends apt-utils && \
    apt-get install -y git && \
    apt-get install -y sudo && \
    apt-get install -y --no-install-recommends gcc && \
    apt-get install -y python-dev && \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
RUN addgroup --gid "${AIRFLOW_GID}" "airflow" && \
    useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME_DIR} -u ${AIRFLOW_UID} -g ${AIRFLOW_GID} airflow && \
    echo 'airflow ALL=(ALL) NOPASSWD: /bin/chown airflow ., /bin/chgrp airflow .' > /etc/sudoers.d/90-airflow && \
    chown -R airflow:airflow ${AIRFLOW_USER_HOME_DIR} && \
    chown -R airflow:airflow ${AIRFLOW_HOME} && \
    chmod +x /entrypoint.sh

EXPOSE 5555 8793

USER airflow
ENTRYPOINT ["/entrypoint.sh"]


