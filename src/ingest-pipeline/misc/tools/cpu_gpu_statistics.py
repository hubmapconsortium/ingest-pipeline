#! /usr/bin/env python

import argparse
import re

from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException, AirflowConfigException
from datetime import datetime
from pathlib import Path
from typing import List


from cryptography.fernet import Fernet
from utils import (
    get_soft_data,
    pythonop_get_dataset_state,
)


def config(section, key):
    dct = airflow_conf.as_dict(display_sensitive=True)
    if section in dct:
        if key in dct[section]:
            rslt = dct[section][key]
        elif key.lower() in dct[section]:
            rslt = dct[section][key.lower()]
        elif key.upper() in dct[section]:
            rslt = dct[section][key.upper()]
        else:
            raise AirflowConfigException("No config entry for [{}] {}".format(section, key))
        # airflow config reader leaves quotes, which we want to strip
        for qc in ['"', "'"]:
            if rslt.startswith(qc) and rslt.endswith(qc):
                rslt = rslt.strip(qc)
        return rslt
    else:
        raise AirflowConfigException("No config section [{}]".format(section))


class CPUGpuStatistics:
    def __get_uuids(self, single_uuid: str = None, **kwargs) -> None:
        """ Queries server to get a full list of processed datasets"""
        uuid_list = []
        self.paths = []
        if single_uuid is None:
            uuid_list = []
        else:
            uuid_list.append(single_uuid)
        fernet = Fernet(config("core", "fernet_key").encode())
        kwargs["crypt_auth_tok"] = fernet.encrypt(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"].encode()).decode()
        for uuid in uuid_list:
            soft_data = get_soft_data(uuid, **kwargs)
            ds_rslt = pythonop_get_dataset_state(
                dataset_uuid_callable=lambda **kwargs: uuid, **kwargs
            )
            if soft_data:
                if soft_data.get("primary") or soft_data.get("assaytype") == "publication":
                    if ds_rslt["creation_action"] == "Multi-Assay Split":
                        print(f"No use for primary data {uuid}")
                else:
                    self.paths.append(ds_rslt["local_directory_full_path"])
            else:
                print(f"No matching soft data returned for {uuid}")
        return

    def __get_timestamp(self, line: str) -> datetime:
        timestamp_format = "%Y-%m-%d %H:%M:%S"
        timestamp_str = line[1:10]
        print(f"Timestamp: {timestamp_str}")
        return datetime.strptime(timestamp_str, timestamp_format)

    def ___calculate_usage(self) -> int:
        usage = self.ending_timestamp - self.starting_timestamp
        self.starting_timestamp = None
        self.ending_timestamp = None
        return usage

    def calculate_statistics(self, single_uuid=None):
        """ Read the session.log file of the dataset and start splitting it between
            [step %] start and [step same %] completed (X) X being success

            First idea, try to get if the job is GPU or CPU, apparently not 100% viable
        """
        startjob = r"[step .+] start$"
        endjob = r"[step .+] completed success$"
        cpu = list
        self.__get_uuids(single_uuid)
        for path in self.paths:
            try:
                with open(Path(path + "session.log"), "r") as session_file:
                    for line in session_file:
                        if re.seach(startjob, line):
                            self.starting_timestamp = self.__get_timestamp(line)
                            # Check if this is CPU or GPU and create a flag
                        if re.seach(endjob, line) and self.starting_timestamp:
                            self.ending_timestamp = self.__get_timestamp(line)
                        if self.starting_timestamp and self.ending_timestamp:
                            # if CPU flag, append to CPU, else append to GPU
                            cpu.append(self.__calculate_usage())
            except FileNotFoundError:
                print(f"{Path(path + 'session.log')} not found")
            except PermissionError:
                print(f"{Path(path + 'session.log')} permission denied")
            except Exception as e:
                print(f"Error {e} in: {Path(path + 'session.log')}")
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--uuid",
        help="UUID that you want to get statitics for!",
    )
    args = parser.parse_args()

    single_uuid = args.uuid

    CPUGpuStatistics().calculate_statistics(single_uuid)


if __name__ == "__main__":
    main()