#!/usr/bin/env python3

# Import modules
import os
import shutil
import re
import tempfile
import hubmap_sdk
import pandas
import sys
import subprocess
import datetime
from hubmap_sdk import EntitySdk


def main(entity_token, usage_csv, log_directory, usage_output):
    # Create temporary file. Then create a copy of each file inside the directory and put it in the temp file.
    temp = tempfile.TemporaryDirectory()
    for filename in os.listdir(log_directory):
        f = os.path.join(log_directory, filename)
        destination = os.path.join(temp.name, filename)
        if filename.endswith(".gz"):
            shutil.copyfile(f, destination)
    subprocess.run(["gunzip", "-r", temp.name])
    # Settings pandas options, initializing dataframe "usage" from csv file, and choosing endpoint/collection ids to track
    pandas.options.display.max_rows = 999999
    pandas.options.display.max_columns = 999999
    usage = pandas.read_csv(usage_csv)
    endpoint_ids = ["24c2ee95-146d-4513-a1b3-ac0bfdb7856f", "af603d86-eab9-4eec-bb1d-9d26556741bb"]
    specific_usage_array = []

    # Filtering out entries from the CSV to only use ones from endpoint ids important to us and within the date range of the
    # log files we have. Taking those rows and columns and creating a new data frame "specific_usage"
    for ind in usage.index:
        is_hubmap = False
        if usage["source_endpoint_id"][ind] in endpoint_ids:
            is_hubmap = True
        if usage["destination_endpoint_id"][ind] in endpoint_ids:
            is_hubmap = True
        if usage["source_endpoint_host_id"][ind] in endpoint_ids:
            is_hubmap = True
        if usage["destination_endpoint_host_id"][ind] in endpoint_ids:
            is_hubmap = True
        request_time = usage['request_time'][ind]
        request_datetime = datetime.datetime.strptime(request_time, '%Y-%m-%d %H:%M:%S.%f')
        timestamp = datetime.datetime.timestamp(request_datetime)
        if is_hubmap and timestamp > 1630468800.0:
            new_series = {
                "user_name": usage["user_name"][ind],
                "request_time": usage["request_time"][ind],
                "source_endpoint_id": usage["source_endpoint_id"][ind],
                "source_endpoint_name": usage["source_endpoint"][ind],
                "destination_endpoint_name": usage["destination_endpoint"][ind],
                "destination_endpoint_id": usage["destination_endpoint_id"][ind],
                "source_endpoint_host_id": usage["source_endpoint_host_id"][ind],
                "destination_endpoint_host_id": usage["destination_endpoint_host_id"][ind],
                "taskid": usage["taskid"][ind],
                "bytes_transferred": usage["bytes_transferred"][ind]
            }
            specific_usage_array.append(new_series)
    specific_usage = pandas.DataFrame(specific_usage_array)

    # Initializing empty lists. hubmap_id_column and data_types_column will be converted into columns for specific_usage
    hubmap_id_column = []
    data_types_column = []
    entity_types_column = []
    rows_to_remove = []
    full_contents = []

    # read each of the files inside temp and keep them in memory:
    for filename in os.listdir(temp.name):
        f = os.path.join(temp.name, filename)
        with open(f, "r") as file:
            lines = file.readlines()
            for each in lines:
                full_contents.append(each)
    temp.cleanup()
    # For every record in the data frame, do:
    count = 0
    for ind in specific_usage.index:
        count = count + 1
        task_id = specific_usage['taskid'][ind]
        lines_with_tid = []
        dataset_id = None
        for line in full_contents:
            index = line.find(task_id)
            if index != -1:
                lines_with_tid.append(line)
        # for every line that contained the task id we are looking for, we now look for the reference number
        for line in lines_with_tid:
            log_entry = line.replace("-","")
            ref_index = log_entry.find("ref=") + 4
            reference_number = log_entry[ref_index: ref_index + 32]
            pattern = 'c_path=.*(\\w{32})'
            ref_output = None
            # once we have the reference number, we once again iterate through the file and look to find where it
            # appears we are looking for the dataset id, which we locate using the above regex pattern
            for b in full_contents:
                ref_index = b.find(reference_number)
                if ref_index == -1:
                    matches = re.search(pattern, b)
                    if matches is not None:
                        ref_output = matches.group(1)
                        break
            if ref_output is not None:
                dataset_id = ref_output
                break

        # Now that we have the dataset id, we can derive hubmap_id, data_type, and entity_type via the Entity Sdk
        hubmap_id = None
        data_type = None
        entity_type = None
        remove_record = False
        if dataset_id is not None:
            try:
                entity_instance = EntitySdk(token=entity_token)
                dataset = entity_instance.get_entity_by_id(dataset_id)
                entity_type = dataset.entity_type
                if entity_type == "Dataset":
                    data_type = dataset.data_types
                else:
                    data_type = "N/A Upload"
                hubmap_id = dataset.hubmap_id
            except hubmap_sdk.sdk_helper.HTTPException as e:
                remove_record = True
        # hubmap public does not record dataset id, so for these transfers we indicate these are unknown.
        public_id = "af603d86-eab9-4eec-bb1d-9d26556741bb"
        if specific_usage["source_endpoint_id"][ind] == public_id or specific_usage["destination_endpoint_id"][ind] == public_id:
            data_type = "Public: Unknown"
            hubmap_id = "Public: Unknown"
            entity_type = "Public: Unknown"
        if remove_record is False:
            data_types_column.append(data_type)
            hubmap_id_column.append(hubmap_id)
            entity_types_column.append(entity_type)
        else:
            rows_to_remove.append(ind)
    # Records that aren't found in entity api are likely removed, so they get dropped from the data frame
    rows_to_remove.reverse()
    for each in rows_to_remove:
        specific_usage.drop(specific_usage.index[each], inplace=True)
    # Add these 3 columns to the data frame specific_usage
    specific_usage['data_type'] = data_types_column
    specific_usage['hubmap_id'] = hubmap_id_column
    specific_usage['entity_type'] = entity_types_column


    # Convert dataframe to json and output with the selected path/name
    specific_usage.to_json(path_or_buf=usage_output, orient='records')

if __name__ == "__main__":
    # Constants. To be obtained from command line
    ENTITY_TOKEN = sys.argv[1]
    USAGE_CSV = sys.argv[2]
    LOG_DIRECTORY = sys.argv[3]
    USAGE_OUTPUT = sys.argv[4]

    main(ENTITY_TOKEN, USAGE_CSV, LOG_DIRECTORY, USAGE_OUTPUT)