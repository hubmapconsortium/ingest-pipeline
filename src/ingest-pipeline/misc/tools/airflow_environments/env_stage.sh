#set airflow environment
HM_AF_CONFIG=/opt/repositories/vm003-stage/ingest-pipeline/src/ingest-pipeline/airflow/airflow.cfg
HM_AF_HOME=/opt/repositories/vm003-stage/ingest-pipeline/src/ingest-pipeline/airflow

# Use DEV since STAGE does not have INGEST API
HM_AF_CONN_INGEST_API_CONNECTION=http://vm001.hive.psc.edu:7777/
HM_AF_CONN_UUID_API_CONNECTION=http://https%3a%2f%2fuuid-api.stage.hubmapconsortium.org/
HM_AF_CONN_FILES_API_CONNECTION=http://https%3a%2f%2ffiles-api.stage.hubmapconsortium.org/
HM_AF_CONN_SPATIAL_API_CONNECTION=http://https%3a%2f%2fspatial-api.stage.hubmapconsortium.org/
HM_AF_CONN_CELLS_API_CONNECTION=http://https%3a%2f%2fcells-api.stage.hubmapconsortium.org/
HM_AF_CONN_SEARCH_API_CONNECTION=http://https%3a%2f%2fontology-api.dev.hubmapconsortium.org/
HM_AF_CONN_ENTITY_API_CONNECTION=http://https%3a%2f%2fentity-api.stage.hubmapconsortium.org/
