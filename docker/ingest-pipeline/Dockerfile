# Parent image
FROM apache/airflow:1.10.11

LABEL description="HuBMAP Ingest Pipeline" \
	version="0.1"

# need git to install other hubmap requirements
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y sudo
RUN echo 'airflow ALL=(ALL) NOPASSWD: /bin/chown airflow ., /bin/chgrp airflow .' > /etc/sudoers.d/90-airflow
COPY entrypoint_wrapper.sh /entrypoint_wrapper.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
RUN chown -R airflow: ${AIRFLOW_HOME}
RUN chmod +x /entrypoint_wrapper.sh

EXPOSE 5555 8793

USER airflow
ENTRYPOINT ["/entrypoint_wrapper.sh"]


