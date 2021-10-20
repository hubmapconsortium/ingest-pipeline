#! /usr/bin/env python

import sys
import argparse
from pprint import pprint
from datetime import date
import pandas as pd
import numpy as np

from hubmap_commons.globus_groups import get_globus_groups_info

from survey import (Entity, Dataset, Sample, EntityFactory,
                    ROW_SORT_KEYS, column_sorter, is_uuid,
                    parse_text_list)


def main():
    """
    main
    """
    # parser = argparse.ArgumentParser()
    # parser.add_argument("uuid_txt",
    #                     help="input .txt file containing uuids or .csv or .tsv file with uuid column")
    # parser.add_argument("--out", help="name of the output .tsv file", required=True)
    # parser.add_argument("--notes", action="append",
    #                     help=("merge dataset notes from this csv/tsv file"
    #                           " (may be repeated)."))
    # args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok, instance='DEV')

    title = 'my dataset'
    contains_human_genetic_sequences = False
    assay_type = 'CODEX'
    type_info = entity_factory.type_client.getAssayType(assay_type)
    pprint(type_info)
    print(type_info.contains_pii)
    direct_ancestors = ['HBM245.ZWNT.288']
    group_uuid = "5bd084c8-edc2-11e8-802f-0e368f3075e8"
    description = "This could in principle be a very long description"

    globus_groups_info = get_globus_groups_info()


    direct_ancestor_uuids = [entity_factory.id_to_uuid(id) for id in direct_ancestors]

    sys.exit('done')

    rslt = entity_factory.create_dataset(
        provider_info=title,
        contains_human_genetic_sequences=contains_human_genetic_sequences,
        assay_type=assay_type,
        direct_ancestor_uuids=direct_ancestor_uuids,
        group_uuid=group_uuid,
        description=description
    )
    


if __name__ == '__main__':
    main()

