#! /usr/bin/env python
import os

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

import argparse
from datetime import date
import pandas as pd
import numpy as np

from survey import (EntityFactory, ROW_SORT_KEYS, column_sorter, is_uuid, parse_text_list, SurveyException)


"""
The survey generates and adds the following notes, so we do not want to propagate
them when they appear in --notes inputs
"""
VOLATILE_NOTES = set(['BAD TYPE NAME',
                      'UUID COLLISION!',
                      'Multiple QA derived datasets',
                      'BAD UUID: No parents?',
                  ])

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
API_SERVICE_NAME = 'sheets'
API_VERSION = 'v4'

# The ID of the official Spreadsheet
# SPREADSHEET_ID = '1-CF0R-rgfmeHMUjLkii7Qv6QbPqMscShJJtLIXB4-74'
# The ID of a test Spreadsheet
SPREADSHEET_ID = '1xTzQRtG841d1ulSM9k-7m3ss1ms4uXPjCtZnEP2QMKA'


def detect_otherdata(ds):
    """
    Returns True if there is at least one file in the dataset directory tree that does not end in '.tsv',
    false otherwise
    """
    for path in ds.full_path.glob('**/*'):
        if path.is_file() and path.suffix != '.tsv':
            return True
    return False


def detect_metadatatsv(ds):
    """
    Returns (True, nrecs) if there is a useable metadata.tsv file in the dataset
    top level directory, or (False, 0) otherwise
    """
    for path in ds.full_path.glob('*metadata.tsv'):
        md_df = pd.read_csv(path, sep='\t')
        if 'assay_type' in md_df.columns:
            return True, len(md_df)
    return False, 0


def detect_clean_validation_report(ds):
    """
    Returns True if there is a validation_report.txt file in the top level
    or extras directory and that file starts with the string 'No errors!',
    or False otherwise.
    """
    rpt_path = ds.full_path / 'validation_report.txt'
    if not rpt_path.is_file():
        rpt_path = ds.full_path / 'extras' / 'validation_report.txt'
    if rpt_path.is_file():
        return rpt_path.read_text().strip() == 'No errors!'
    else:
        return False


def get_most_recent_touch(ds):
    """
    Given a dataset, descend through directories finding the one with the most recent
    ctime and return it as a date string.
    """
    ctime = ds.full_path.stat().st_ctime
    for subdir in ds.full_path.glob('**/'):
        ctime = max(ctime, subdir.stat().st_ctime)
    return str(date.fromtimestamp(ctime))


def data_type_resolver(row):
    dt_x = parse_text_list(row["data_types_x"])
    dt_y = parse_text_list(row["data_types_y"])
    if (isinstance(dt_x, str) and dt_x.lower() == 'nan') or (isinstance(dt_x, float) and dt_x == np.nan):
        if (isinstance(dt_y, str) and dt_y.lower() == 'nan') or (isinstance(dt_y, float) and dt_y == np.nan):
            return '????'
        else:
            return f'{dt_y}'
    else:
        if dt_x == dt_y:
            return f'{dt_x}'
        else:
            print(f'unreconcilable datatypes: {dt_x} {type(dt_x)} {dt_y} {type(dt_y)}')
            return f'{dt_x}:{dt_y}'


def _merge_note_pair(row):
    note_x, note_y = row['note_x'], row['note_y']
    if note_x == 'nan':  # pandas made me do it!  It's not my fault!
        note_x = ''
    if note_y == 'nan':
        note_y = ''
    words_x = [word.strip() for word in note_x.split(';')]
    words_x = [word for word in words_x if word]
    words_y = [word.strip() for word in note_y.split(';')]
    words_y = [word for word in words_y if word and word not in VOLATILE_NOTES]
    dedup_words = []
    for word in words_x + words_y:
        if word not in dedup_words:
            dedup_words.append(word)
    return ';'.join(dedup_words)


def join_notes(df, notes_df):
    df = pd.merge(df, notes_df[['uuid', 'note']].drop_duplicates(), on='uuid', how='left')
    assert 'note_x' in df.columns and 'note_y' in df.columns, "cannot find the notes to merge"
    note_df = df[['note_x', 'note_y']].astype(str)
    df['note'] = note_df.apply(_merge_note_pair, axis=1)
    return df.drop(columns=['note_x', 'note_y'])


def get_google_service():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('.token.json'):
        creds = Credentials.from_authorized_user_file('.token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '.sheetreader_key.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        os.umask(0)
        with open(os.open('.token.json', os.O_CREAT | os.O_WRONLY, 0o200), 'w') as token:
            token.write(creds.to_json())
    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds)
        return service
    except Exception as e:
        print(f'Error {e}')
    return None


def get_google_last_sheet(notes_sheet):
    service = get_google_service()
    if service:
        try:
            # Call the Sheets API
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                        range=notes_sheet).execute()
            header = result.get('values', [])[0]
            values = result.get('values', [])[1:]

            if not values:
                print('No data found.')
                return
            else:
                df = pd.DataFrame(data=values, columns=header)
                df.fillna("", inplace=True)
                return df
        except HttpError as e:
            print(f'Error {e}')
    return


def insert_google_sheet(df, outfile):
    service = get_google_service()
    if service:
        try:
            sheet = service.spreadsheets()
            body = {
                'requests': [
                    {
                        'addSheet': {
                            'properties': {
                                'title': outfile,
                                'index': 1
                            }
                        }
                    }
                ]
            }
            response = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            sheet_id = response.get('replies')[0].get('addSheet').get('properties').get('sheetId')
            body = {
                'requests': [
                    {
                        'pasteData': {
                            'coordinate': {
                                'sheetId': sheet_id,
                                'rowIndex': 0,
                                'columnIndex': 0,
                            },
                            'data': df.to_csv(sep='\t', columns=column_sorter([elt for elt in df.columns])),
                            'delimiter': '\t'
                        }
                    }
                ]
            }
            # Pandas create a new column for row indices which needs to be removed from the actual sheet
            sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            body = {
                'requests': [
                    {
                        'deleteRange': {
                            'range': {
                                'sheetId': sheet_id,
                                'startColumnIndex': 0,
                                'endColumnIndex': 1
                            },
                            'shiftDimension': 'COLUMNS'
                        }
                    }
                ]
            }
            sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
            return
        except HttpError as e:
            print(f'Error {e}')
    else:
        return


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('uuid_txt', nargs='?',
                        help=("Optional input .txt file containing uuids"
                              " or .csv or .tsv file with uuid column."
                              " The default is to get this data from entity-api."
                              ))
    parser.add_argument('--out_sheet', help="name of the sheet to create")
    parser.add_argument('--notes_sheet', action='store',
                        help="name of the sheet currently on the Google Drive to take the notes from")
    parser.add_argument('--to_tsv', action='store_true')
    parser.add_argument('--out_file', help="path of the file to create")
    parser.add_argument('--notes_file', action='store',
                        help="path to the file to take the notes from")
    args = parser.parse_args()
    if args.to_tsv and not args.out_file:
        raise RuntimeError(f"If you want to generate a tsv file you need to indicate the output path with --out_file")
    if not args.to_tsv and not args.out_sheet:
        raise RuntimeError(f"If you want to generate a new sheet on Google Drive you need to indicate the output sheet "
                           f"name with --out_sheet")
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)

    uuid_l = []
    if not args.uuid_txt:
        in_df = entity_factory.fetch_new_dataset_table()
        assert 'uuid' in in_df.columns, ('"uuid" column is missing from table'
                                         ' provided by entity-api?')
        uuid_key = 'uuid'
        for elt in in_df['uuid']:
            uuid_l.append(str(elt))
    elif args.uuid_txt.endswith((".csv", ".tsv")):
        in_df = pd.read_csv(args.uuid_txt, engine="python", sep=None,
                            dtype={'note': np.str}, encoding='utf-8-sig')
        if 'uuid' in in_df.columns:
            uuid_key = 'uuid'
        elif 'e.uuid' in in_df.columns:
            uuid_key = 'e.uuid'
        else:
            raise RuntimeError(f'Cannot find uuid column in {args.uuid_txt}')
        for elt in in_df[uuid_key]:
            uuid_l.append(str(elt))
    else:
        in_df = None
        with open(args.uuid_txt) as f:
            for line in f:
                uuid = None
                if is_uuid(line.strip()):
                    uuid = line.strip()
                else:
                    words = line.strip().split()
                    for word in words:
                        a, b = word.split(':')
                        if a.lower() == 'uuid':
                            uuid = b
                            break
                if uuid:
                    uuid_l.append(uuid)
                    print(f'{uuid}')
                else:
                    print(f'cannot find uuid in {line.strip()}')

    out_recs = []

    known_uuids = set()
    for uuid in uuid_l:
        rec = {}
        try:
            ds = entity_factory.get(uuid)
            ds.describe()
            new_uuids = ds.all_dataset_uuids()
            try:
                rec = ds.build_rec()
                rec['has_metadata'], rec['n_md_recs'] = detect_metadatatsv(ds)
                rec['has_data'] = detect_otherdata(ds)
                rec['validated'] = detect_clean_validation_report(ds)
                try:
                    rec['last_touch'] = get_most_recent_touch(ds)
                except OSError as e:
                    rec['last_touch'] = f'OSError: {e}'
                if any([uuid in known_uuids for uuid in new_uuids]):
                    rec['note'] = 'UUID COLLISION! '
                    print(f'collision on {[uuid for uuid in new_uuids if uuid in known_uuids]}')
                    known_uuids = known_uuids.union(new_uuids)
            except AssertionError as e:
                old_note = rec['note'] if 'note' in rec else ''
                rec['note'] = f'BAD UUID: {e} ' + old_note
                rec['uuid'] = uuid  # just to make sure it is present
        except SurveyException as e:
            print(f'dropping {uuid} because {e}')
            rec['uuid'] = uuid
            rec['note'] = f'not in survey because {e}'
        if rec:
            out_recs.append(rec)
    out_df = pd.DataFrame(out_recs).rename(columns={'qa_child_uuid': 'derived_uuid',
                                                    'qa_child_hubmap_id': 'derived_hubmap_id',
                                                    'qa_child_data_type': 'derived_data_type',
                                                    'qa_child_status': 'derived_status'})
    if in_df is not None:
        out_df = out_df.drop_duplicates().merge(in_df.drop_duplicates(), left_on='uuid', right_on=uuid_key)

    # Some cleanup on out_df before we save it
    drop_list = []
    rename_d = {}
    for col in out_df.columns:
        if col.startswith('derived_'):
            drop_list.append(col)
    if 'group_name' in out_df.columns and 'organization' in out_df.columns:
        drop_list.append('organization')
    if 'hubmap_id_x' in out_df.columns and 'hubmap_id_y' in out_df.columns:
        if (out_df['hubmap_id_y'].isnull() | (out_df['hubmap_id_x'] == out_df['hubmap_id_y'])).all():
            drop_list.append('hubmap_id_y')
            rename_d['hubmap_id_x'] = 'hubmap_id'
        else:
            print('ALERT! hubmap_id mismatch mismatch!')
            out_df.to_csv('/tmp/debug_out_df.tsv', sep='\t')
            drop_list.append('hubmap_id_y')
            rename_d['hubmap_id_x'] = 'hubmap_id'
    if 'data_types_x' in out_df.columns and 'data_types_y' in out_df.columns:
        out_df['data_types'] = out_df[['data_types_x', 'data_types_y']].apply(data_type_resolver, axis=1)
        drop_list.extend(['data_types_x', 'data_types_y'])
    out_df = out_df.drop(drop_list, axis=1)
    if rename_d:
        out_df = out_df.rename(columns=rename_d)

    if args.to_tsv and args.notes_file:
        notes_df = pd.read_csv(args.notes_file, engine='python', sep=None,
                               dtype={'note': np.str}, encoding='utf-8-sig')
        notes = True
    elif not args.to_tsv and hasattr(args, "notes_sheet"):
        notes_df = get_google_last_sheet(args.notes_sheet)
        notes = True
    else:
        print('No notes')
        notes = False
    if notes:
        for elt in ['uuid', 'note']:
            if elt not in notes_df.columns:
                print(f'ERROR: notes file does not contain {elt}, so notes were not merged')
                break
        else:
            out_df = join_notes(out_df, notes_df)

    out_df = out_df.sort_values([key for key in ROW_SORT_KEYS if key in out_df.columns],
                                axis=0)

    if args.to_tsv:
        out_df.to_csv(args.out_file, sep='\t', index=False,
                      columns=column_sorter([elt for elt in out_df.columns])
                      )
    else:
        insert_google_sheet(out_df, args.out_sheet)


if __name__ == '__main__':
    main()
