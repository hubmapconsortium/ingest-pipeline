import logging
import os
from datetime import datetime
from functools import cached_property

import asana
from status_manager import StatusChangerException, Statuses
from status_utils import get_hubmap_id_from_uuid, get_submission_context


class UpdateAsana:
    def __init__(
        self,
        uuid: str | None,
        token: str,
        status: Statuses | None,
        workspace_id: str = os.environ["WORKSPACE_ID"],
        project_id: str = os.environ["PROJECT_ID"],
    ):
        self.workspace = workspace_id
        self.project = project_id
        self.client = asana.Client.access_token(os.environ["ASANA_API_KEY"])
        self.status = status
        self.token = token
        self.uuid = uuid
        self.custom_fields = self.get_custom_fields()
        self.process_stage_field_gid = self.get_custom_field_gid("Process Stage")

    @cached_property
    def asana_status_map(self):
        return {
            Statuses.DATASET_ERROR: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_INVALID: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_NEW: self.process_stage_gids["Intake"],
            Statuses.DATASET_PROCESSING: "",
            Statuses.DATASET_PUBLISHED: self.process_stage_gids["Publishing"],
            Statuses.DATASET_QA: self.process_stage_gids["Post-Processing"],
            Statuses.UPLOAD_ERROR: self.process_stage_gids["Blocked 🛑"],
            Statuses.UPLOAD_INVALID: self.process_stage_gids["Blocked 🛑"],
            Statuses.UPLOAD_NEW: self.process_stage_gids["Intake"],
            Statuses.UPLOAD_PROCESSING: "",
            Statuses.UPLOAD_REORGANIZED: self.process_stage_gids["Processing"],
            Statuses.UPLOAD_SUBMITTED: self.process_stage_gids["Pre-Processing"],
            Statuses.UPLOAD_VALID: self.process_stage_gids["Pre-Processing"],
        }

    @cached_property
    def hubmap_id(self):
        if self.uuid is not None:
            return get_hubmap_id_from_uuid(self.token, self.uuid)

    @cached_property
    def submission_data(self):
        if self.uuid is not None:
            return get_submission_context(self.token, self.uuid)
        else:
            raise Exception("Cannot fetch data from Entity API. No uuid passed.")

    def get_custom_fields(self):
        return list(
            self.client.custom_field_settings.get_custom_field_settings_for_project(self.project)
        )

    def get_custom_field_gid(self, custom_field_name: str) -> str:
        for custom_field in self.custom_fields:
            try:
                name = custom_field["custom_field"]["name"]
            except Exception:
                continue
            if name == custom_field_name:
                if name == "Process Stage":
                    self.process_stage_gids = {}
                    for stage in custom_field["custom_field"]["enum_options"]:
                        self.process_stage_gids[stage["name"]] = stage["gid"]
                elif name == "Entity Type":
                    self.entity_type_gids = {}
                    for stage in custom_field["custom_field"]["enum_options"]:
                        self.process_stage_gids[stage["name"]] = stage["gid"]
                return custom_field["custom_field"]["gid"]
        raise Exception(f"Asana custom field {custom_field_name} not found.")

    @cached_property
    def get_task_by_hubmap_id(self) -> str:
        """
        Given a HuBMAP ID, find the associated Asana task.
        Fails if a HuBMAP ID is associated with 0 or >1 tasks.
        """
        hubmap_id_field = self.get_custom_field_gid("HuBMAP ID")
        response_list = list(
            self.client.tasks.search_tasks_for_workspace(
                self.workspace,
                {
                    "projects_any": self.project,
                    f"custom_fields.{hubmap_id_field}.contains": self.hubmap_id,
                    "opt_fields": ["gid", "custom_fields"],
                },
            )
        )
        response_length = len(response_list)
        if response_length == 1:
            task_id = response_list[0]["gid"]
        # TODO: this is not so great
        elif response_length > 1:
            types = []
            gids = []
            for response in response_list:
                for field in response["custom_fields"]:
                    if (
                        field["name"] == "Entity Type"
                        and (entity_type := field["enum_value"]["name"]) == "Dataset"
                    ):
                        types.append(entity_type)
                        gids.append(response["gid"])
            if len(types) > 1:
                task_gids = [task["gid"] for task in response_list]
                raise StatusChangerException(
                    f"""Multiple tasks with the entity_type tag 'Dataset' found
                    for HuBMAP ID {self.hubmap_id}. GIDs found: {task_gids}"""
                )
            elif len(types) == 0:
                raise StatusChangerException(
                    f"No tasks with type 'Dataset' found for HuBMAP ID {self.hubmap_id}."
                )
            task_id = gids[0]
        else:
            logging.info(
                f"""
                Error retrieving task by HuBMAP ID for {self.hubmap_id}!
                {response_length} results found.
                Check that a task for the expected HuBMAP ID exists in Asana and that
                it is formatted '{self.hubmap_id}' with no surrounding whitespace.
                """
            )
            return ""
        return task_id

    @cached_property
    def get_asana_status(self) -> str:
        """
        Maps between enum for Entity API status that was passed in
        and Asana status to be set.
        """
        if self.status not in self.asana_status_map:
            raise StatusChangerException(
                f"""Status {self.status} assigned to {self.hubmap_id}
                not found in asana_status_map. Status not updated."""
            )
        asana_status = self.asana_status_map[self.status]
        return asana_status

    def update_process_stage(self) -> None:
        if self.status in [None, Statuses.UPLOAD_PROCESSING, Statuses.DATASET_PROCESSING]:
            return
        elif self.status == Statuses.UPLOAD_REORGANIZED:
            self.create_dataset_cards()
        try:
            response = self.client.tasks.update_task(
                self.get_task_by_hubmap_id,
                {
                    "custom_fields": {
                        self.get_custom_field_gid("Process Stage"): self.get_asana_status
                    },
                },
                opt_pretty=True,
            )
        # Separating this one out to handle empty string passed as task_id because task wasn't found
        except NotFoundError as e:
            logging.info(
                f"""
                    Error occurred while updating Asana status for HuBMAP ID {self.hubmap_id};
                    task ID that was passed was {self.get_task_by_hubmap_id}.
                    Task status not updated, may need to be updated manually.
                    Error: {e}
                """
            )
            return
        except Exception as e:
            logging.info(
                f"""
                    Error occurred while updating Asana status for HuBMAP ID {self.hubmap_id}.
                    Task status not updated, may need to be updated manually.
                    Error: {e}
                """
            )
            return
        try:
            new_status = list(
                field["enum_value"]
                for field in response["custom_fields"]
                if field["name"] == "Process Stage"
            )[0]
            assert (
                self.get_asana_status == new_status["gid"]
            ), f"""
                Asana status matching Entity API status '{self.status}' not applied
                to {self.hubmap_id}. Current status in Asana matches GID {new_status['name']}.
                """
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            raise StatusChangerException(
                f"""Error occurred while updating Asana status for HuBMAP ID {self.hubmap_id}.
                Status not updated.
                Error: {e}"""
            )

    def convert_utc_timestamp(self, dataset):
        # Convert UTC timestamp in milliseconds to readable date
        timestamp = datetime.utcfromtimestamp(int(dataset["created_timestamp"]) / 1000).strftime(
            "%Y%m%d"
        )
        return timestamp

    def create_dataset_cards(self):
        child_datasets = [dataset for dataset in self.submission_data["datasets"]]
        logging.info(
            f"""Upload {self.hubmap_id} has {len(child_datasets)} child datasets.
            Creating Asana cards..."""
        )
        for dataset in child_datasets:
            timestamp = self.convert_utc_timestamp(dataset)
            try:
                response = self.client.tasks.create_task(
                    {
                        "name": f"{dataset['group_name']} | {dataset['ingest_metadata']['metadata']['assay_type']} | {timestamp}",  # noqa
                        "custom_fields": {
                            # TODO: should assay type pull from dataset['data_types']
                            # or dataset['ingest_metadata']['metadata']['assay_type']?
                            self.get_custom_field_gid("HuBMAP ID"): dataset["hubmap_id"],
                            # TODO: is "New" correct? Should it be mapped to Intake?
                            self.get_custom_field_gid("Process Stage"): self.process_stage_gids[
                                "Intake"
                            ],
                            # self.get_custom_field_gid("Entity Type"): self.entity_type_gids[
                            #     "dataset"
                            # ],
                            # TODO: does parent_field_gid need to be extensible for card IDs
                            # for non-upload groups? is that a separate field or is this actually
                            # a parent milestone ID field?
                            # self.get_custom_field_gid("Parent"): self.hubmap_id,
                        },
                        "projects": [self.project],
                    },
                    opt_pretty=True,
                )
                response.raise_for_status()
                logging.info(
                    f"""Card created successfully for dataset {dataset['hubmap_id']}.
                             Response:
                             {response.json()}"""
                )
            except Exception:
                raise StatusChangerException(
                    f"""Error creating card for dataset {dataset['hubmap_id']},
                    part of reorganized dataset {self.hubmap_id}"""
                )
        logging.info(f"All dataset cards created for upload {self.hubmap_id}")
