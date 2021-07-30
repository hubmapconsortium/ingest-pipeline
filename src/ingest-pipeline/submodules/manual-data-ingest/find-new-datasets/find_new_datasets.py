import neo4j
import configparser

FIND_NEW_DATASETS_Q = "match (ds:Entity {entitytype:'Dataset'})-[:HAS_METADATA]->(m:Metadata) where m.status = 'New' return m.data_types as data_types, m.provenance_group_name as organization, ds.uuid as uuid, ds.hubmap_identifier as hubmap_id"

config = configparser.ConfigParser()
config.read('find_new_datasets.properties')
props=config['NEO4J']
neo = neo4j.GraphDatabase.driver(props.get('neo4j.server'), auth=(props.get('neo4j.username'), props.get('neo4j.password')))

def data_types_to_str(data_types):
    if data_types is None: return ''
    r_val = ''
    comma = ''
    first = True
    for dt in data_types:
        r_val = comma + dt
        if first:
            first = False
            comma = ', '

with neo.session() as session:
    recds = session.run(FIND_NEW_DATASETS_Q)
    for record in recds:
        print(record.keys())
        print(f"UUID:{record.get('uuid')} HUBMAP_ID:{record.get('hubmap_id')} DATA_TYPES:{data_types_to_str(record.get('data_types'))} ORGANIZATION:{record.get('organization')}")
        
