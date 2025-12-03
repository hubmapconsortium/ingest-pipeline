#! /usr/bin/env python

import sys
import argparse
import re
import json
from pathlib import Path
import pandas as pd
import requests

from hubmap_commons.hm_auth import AuthHelper

# No trailing slashes in the following URLs!
ENDPOINTS = {
    "PROD": {
        "entity_url": "https://entity.api.hubmapconsortium.org",
        "search_url": "https://search.api.hubmapconsortium.org/v3",
        "ingest_url": "http://vm004.hive.psc.edu:7777",
        "assay_info_url": "https://search.api.hubmapconsortium.org/v3",
    },
    "STAGE": {
        "entity_url": "https://entity-api.stage.hubmapconsortium.org",
        "search_url": "https://search-api.stage.hubmapconsortium.org/v3",
        "ingest_url": "http://vm003.hive.psc.edu:7777",
        "assay_info_url": "https://search-api.stage.hubmapconsortium.org/v3",
    },
    "TEST": {
        "entity_url": "https://entity-api.test.hubmapconsortium.org",
        "search_url": "https://search-api.test.hubmapconsortium.org/v3",
        "ingest_url": "http://vm002.hive.psc.edu:7777",
        "assay_info_url": "https://search-api.test.hubmapconsortium.org/v3",
    },
    "DEV": {
        "entity_url": "https://entity-api.dev.hubmapconsortium.org",
        "search_url": "https://search-api.dev.hubmapconsortium.org/v3",
        "ingest_url": "http://vm001.hive.psc.edu:7777",
        "assay_info_url": "https://search-api.dev.hubmapconsortium.org/v3",
    },
}


STRIP_DOUBLEQUOTES_RE = re.compile(r'"(.*)"')
STRIP_QUOTES_RE = re.compile(r"'(.*)'")
STRIP_BRACKETS_RE = re.compile(r"\[(.*)\]")


#
# Large negative numbers move columns left, large positive numbers move them right.
# Columns for which no weight is given end up with weight 0, so they sort to the
# middle alphabetically.
#
COLUMN_SORT_WEIGHTS = {
    "note": 20,
    "validated": 10,
    "last_touch": 11,
    "n_md_recs": 9,
    "has_metadata": 8,
    "has_data": 7,
    "group_name": -10,
    "data_types": -9,
    "uuid": -8,
    "hubmap_id": -7,
}


#
# Column labels to be used as keys in sorting rows
#
ROW_SORT_KEYS = ["is_derived", "group_name", "data_types", "status", "uuid"]


#
# Some Entities are worth caching
#
ENTITY_CACHE = {}


class SurveyException(AssertionError):
    pass


def parse_text_list(s):
    s = str(s) if s is not None else ""
    this_s = s
    while True:
        if isinstance(s, list):
            s = [parse_text_list(elt) for elt in s]
        else:
            for regex in [STRIP_QUOTES_RE, STRIP_DOUBLEQUOTES_RE, STRIP_BRACKETS_RE]:
                m = regex.match(s)
                if m:
                    s = m.group(1)
            if "," in s:
                s = [parse_text_list(word.strip()) for word in s.split(",")]
        if s == this_s:
            return s
        this_s = s


def column_sorter(col_l):
    sort_me = [
        ((COLUMN_SORT_WEIGHTS[key] if key in COLUMN_SORT_WEIGHTS else 0), key) for key in col_l
    ]
    return [key for wt, key in sorted(sort_me)]


class SplitTree:
    def __init__(self):
        self.root = {}

    def add(self, s):
        words = s.strip().split("-")
        here = self.root
        while words:
            w0 = words.pop(0)
            if w0 not in here:
                here[w0] = {}
            here = here[w0]

    def _inner_dump(self, dct, prefix):
        for elt in dct:
            print(f"{prefix}{elt}:")
            self._inner_dump(dct[elt], prefix + "    ")

    def dump(self):
        self._inner_dump(self.root, "")

    def _inner_str(self, dct, prefix=""):
        k_l = sorted(dct)
        if k_l:
            if len(k_l) == 1:
                next_term = self._inner_str(dct[k_l[0]], prefix=prefix + "  ")
                rslt = k_l[0]
                if next_term:
                    rslt += "-" + next_term
                return rslt
            kid_l = [self._inner_str(dct[k], prefix=prefix + "  ") for k in k_l]
            if all([k == "" for k in kid_l]):
                s = ",".join(k_l)
            else:
                s = ",".join([f"{a}-{b}" for a, b in zip(k_l, kid_l)])
            rslt = f"[{s}]"
            return rslt
        return ""

    def __str__(self):
        return self._inner_str(self.root)


class Entity:
    def __init__(self, prop_dct, entity_factory):
        self.uuid = prop_dct["uuid"]
        self.prop_dct = prop_dct
        self.entity_factory = entity_factory
        self.hubmap_id = prop_dct["hubmap_id"]
        self.notes = []

    def describe(self, prefix="", file=sys.stdout):
        print(f"{prefix}{self.uuid}: " f"{self.hubmap_id} ", file=file)

    def all_uuids(self):
        """
        Returns a list of unique UUIDs associated with this entity
        """
        return [self.uuid]

    def all_dataset_uuids(self):
        """
        Returns a list of unique dataset UUIDs associated with this entity
        """
        return []


class Dataset(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        if prop_dct["entity_type"] not in ["Dataset", "Support", "Publication"]:
            raise AssertionError(f"uuid {prop_dct['uuid']} is a" f"{prop_dct['entity_type']}")
        self.status = prop_dct["status"]
        if "metadata" in prop_dct and prop_dct["metadata"]:
            if "dag_provenance_list" in prop_dct["metadata"]:
                dag_prv = prop_dct["metadata"]["dag_provenance_list"]
            else:
                dag_prv = None
        else:
            dag_prv = None
        self.dag_provenance = dag_prv
        direct_ancestors_list = prop_dct.get("direct_ancestors", [])
        self.parent_uuids = [elt["uuid"] for elt in direct_ancestors_list]
        self.parent_dataset_uuids = [
            elt["uuid"]
            for elt in direct_ancestors_list
            if elt["entity_type"] in ["Dataset", "Publication"]
        ]
        direct_descendants_list = prop_dct.get("direct_descendants", [])
        self.kid_uuids = [elt["uuid"] for elt in direct_descendants_list]
        self.kid_dataset_uuids = [
            elt["uuid"]
            for elt in direct_descendants_list
            if elt["entity_type"] in ["Dataset", "Publication"]
        ]
        self.data_types = prop_dct["data_types"] if "data_types" in prop_dct else []
        assay_type = parse_text_list(self.data_types)
        if isinstance(assay_type, list) and len(assay_type) == 1:
            assay_type = assay_type[0]
        try:
            type_info = self.entity_factory.type_client.getAssayType(assay_type)
            self.data_types = type_info.name
            self.is_derived = not type_info.primary
        except Exception:
            self.data_types = f"{self.data_types}"
            self.notes.append("BAD TYPE NAME")
            self.is_derived = None
        self._donor_uuid = None  # requires graph search, so be lazy
        self.group_name = prop_dct["group_name"]
        if self.group_name.startswith("hubmap-"):  # it's actually a group id
            self.group_name = AuthHelper.getGroupDisplayName(prop_dct["group_uuid"])
        c_h_g = prop_dct["contains_human_genetic_sequences"]
        if isinstance(c_h_g, str):
            c_h_g = c_h_g.lower() in ["yes", "true"]
        self.contains_human_genetic_sequences = c_h_g
        self._kid_dct = None
        self._parent_dct = None
        self._organs = None

    @property
    def donor_uuid(self):
        if self._donor_uuid is None and self.parent_uuids:
            parent_uuid = self.parent_uuids[0]
            parent_entity = self.entity_factory.get(parent_uuid)
            if isinstance(parent_entity, Donor):
                self._donor_uuid = parent_uuid
            else:
                parent_donor_uuid = parent_entity.donor_uuid
                self._donor_uuid = parent_donor_uuid
        return self._donor_uuid

    @property
    def organs(self):
        if self._organs is None:
            organ_list = []
            for parent_uuid in self.parent_uuids:
                parent_entity = self.entity_factory.get(parent_uuid)
                if isinstance(parent_entity, Sample):
                    organ_list.append(parent_entity.organ)
                elif isinstance(parent_entity, Donor):
                    raise SurveyException(f"Parent of Dataset {self.uuid} is a Donor!")
                else:
                    organ_list.extend(parent_entity.organs)
            self._organs = list(set(organ_list))
        return self._organs

    @property
    def kids(self):
        if self._kid_dct is None:
            self._kid_dct = {uuid: self.entity_factory.get(uuid) for uuid in self.kid_uuids}
        return self._kid_dct

    @property
    def parents(self):
        if self._parent_dct is None:
            self._parent_dct = {uuid: self.entity_factory.get(uuid) for uuid in self.parent_uuids}
        return self._parent_dct

    @property
    def full_path(self):
        return self.entity_factory.get_full_path(self.uuid)

    def describe(self, prefix="", file=sys.stdout):
        print(
            f"{prefix}Dataset {self.uuid}: "
            f"{self.hubmap_id} "
            f"{self.data_types} "
            f"{self.status} "
            f"{self.notes if self.notes else ''}",
            file=file,
        )
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix + "    ", file=file)

    def _parse_sample_parents(self):
        other_parent_uuids = [
            uuid for uuid in self.parent_uuids if uuid not in self.parent_dataset_uuids
        ]
        assert not (self.parent_dataset_uuids and other_parent_uuids), (
            "All parents should be datasets, or" " no parent should be a dataset"
        )
        if other_parent_uuids:
            s_t = SplitTree()
            for p_uuid in other_parent_uuids:
                samp = self.entity_factory.get(p_uuid)
                assert isinstance(samp, (Sample, Donor)), "was expecting a sample or donor?"
                s_t.add(samp.submission_id)
            sample_submission_id = str(s_t)
            sample_hubmap_id = samp.hubmap_id if len(other_parent_uuids) == 1 else "multiple"
            parent_dataset = None
        else:
            sample_submission_id = sample_hubmap_id = None
            parent_dataset = (
                self.parent_dataset_uuids[0] if len(self.parent_dataset_uuids) == 1 else "multiple"
            )
        return parent_dataset, sample_submission_id, sample_hubmap_id

    def build_rec(self, include_all_children=False):
        """
        Returns a dict containing:

        uuid
        hubmap_id
        data_types[0]  (verifying there is only 1 entry)
        status
        QA_child.uuid
        QA_child.hubmap_id
        QA_child.data_types[0]  (verifying there is only 1 entry)
        QA_child.status   (which must be QA or Published)
        note

        If include_all_children=True, all child datasets are included rather
        than just those that are QA or Published.
        """
        rec = {
            "uuid": self.uuid,
            "hubmap_id": self.hubmap_id,
            "status": self.status,
            "group_name": self.group_name,
            "is_derived": self.is_derived,
        }
        if not self.data_types:
            rec["data_types"] = "[]"
        elif len(self.data_types) == 1:
            rec["data_types"] = self.data_types[0]
        elif isinstance(self.data_types, str):
            rec["data_types"] = self.data_types
        else:
            rec["data_types"] = f"[{','.join(self.data_types)}]"
        (rec["parent_dataset"], rec["sample_submission_id"], rec["sample_hubmap_id"]) = (
            self._parse_sample_parents()
        )
        rec["donor_uuid"] = self.donor_uuid
        if not self.organs:
            raise SurveyException(f"The source organ for Sample {self.uuid} could not be found")
        if len(self.organs) > 1:
            raise SurveyException(
                f"Sample {self.uuid} comes from multiple organs," " which is not supported"
            )
        if include_all_children:
            filtered_kids = [self.kids[uuid] for uuid in self.kids]
            uuid_hdr, doi_hdr, data_type_hdr, status_hdr, note_note = (
                "child_uuid",
                "child_hubmap_id",
                "child_data_type",
                "child_status",
                "Multiple derived datasets",
            )
        else:
            filtered_kids = [
                self.kids[uuid]
                for uuid in self.kids
                if self.kids[uuid].status in ["QA", "Published"]
            ]
            (uuid_hdr, doi_hdr, data_type_hdr, status_hdr, note_note) = (
                "qa_child_uuid",
                "qa_child_hubmap_id",
                "qa_child_data_type",
                "qa_child_status",
                "Multiple QA derived datasets",
            )
        if any(filtered_kids):
            rec["note"] = note_note if len(filtered_kids) > 1 else ""
            this_kid = filtered_kids[0]
            rec[uuid_hdr] = this_kid.uuid
            rec[doi_hdr] = this_kid.hubmap_id
            rec[data_type_hdr] = this_kid.data_types[0]
            rec[status_hdr] = this_kid.status
        else:
            for key in [uuid_hdr, doi_hdr, data_type_hdr, status_hdr]:
                rec[key] = None
            rec["note"] = ""
        rec["note"] = "; ".join([rec["note"]] + self.notes)
        if "upload" in self.prop_dct:
            rec["upload"] = self.prop_dct["upload"]["uuid"]
        else:
            rec["upload"] = None

        return rec

    def all_uuids(self):
        """
        Returns a list of unique UUIDs associated with this entity
        """
        return super().all_uuids() + self.kid_uuids + self.parent_uuids

    def all_dataset_uuids(self):
        """
        Returns a list of unique dataset or support UUIDs associated with this entity
        """
        return super().all_dataset_uuids() + self.kid_dataset_uuids + self.parent_dataset_uuids


class Publication(Dataset):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct["entity_type"] == "Publication", (
            f"uuid {prop_dct['uuid']} is" f" a {prop_dct['entity_type']}"
        )

    def describe(self, prefix="", file=sys.stdout):
        print(
            f"{prefix}Publication {self.uuid}: "
            f"{self.hubmap_id} "
            f"{self.data_types} "
            f"{self.status} "
            f"{self.notes if self.notes else ''}",
            file=file,
        )
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix + "    ", file=file)


class Upload(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct["entity_type"] in ["Upload"], (
            f"uuid {prop_dct['uuid']} is a" f"{prop_dct['entity_type']}"
        )
        self.status = prop_dct["status"]
        if "metadata" in prop_dct and prop_dct["metadata"]:
            if "dag_provenance_list" in prop_dct["metadata"]:
                dag_prv = prop_dct["metadata"]["dag_provenance_list"]
            else:
                dag_prv = None
        else:
            dag_prv = None
        self.dag_provenance = dag_prv
        direct_ancestors_list = prop_dct.get("direct_ancestors", [])
        self.parent_uuids = [elt["uuid"] for elt in direct_ancestors_list]
        self.parent_dataset_uuids = [
            elt["uuid"] for elt in direct_ancestors_list if elt["entity_type"] == "Dataset"
        ]
        direct_descendants_list = prop_dct.get("direct_descendants", [])
        self.kid_uuids = [elt["uuid"] for elt in direct_descendants_list]
        self.kid_dataset_uuids = [
            elt["uuid"] for elt in direct_descendants_list if elt["entity_type"] == "Dataset"
        ]
        self.group_name = prop_dct["group_name"]
        self._kid_dct = None
        self._parent_dct = None

    @property
    def kids(self):
        if self._kid_dct is None:
            self._kid_dct = {uuid: self.entity_factory.get(uuid) for uuid in self.kid_uuids}
        return self._kid_dct

    @property
    def parents(self):
        if self._parent_dct is None:
            self._parent_dct = {uuid: self.entity_factory.get(uuid) for uuid in self.parent_uuids}
        return self._parent_dct

    @property
    def full_path(self):
        return self.entity_factory.get_full_path(self.uuid)

    def describe(self, prefix="", file=sys.stdout):
        print(
            f"{prefix}Upload {self.uuid}: "
            f"{self.hubmap_id} "
            f"{self.status} "
            f"{self.notes if self.notes else ''}",
            file=file,
        )
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix + "    ", file=file)

    def _parse_sample_parents(self):
        other_parent_uuids = [
            uuid for uuid in self.parent_uuids if uuid not in self.parent_dataset_uuids
        ]
        assert not (self.parent_dataset_uuids and other_parent_uuids), (
            "All parents should be datasets, or" " no parent should be a dataset"
        )
        if other_parent_uuids:
            s_t = SplitTree()
            for p_uuid in other_parent_uuids:
                samp = self.entity_factory.get(p_uuid)
                assert isinstance(samp, (Sample, Donor)), "was expecting a sample or donor?"
                s_t.add(samp.submission_id)
            sample_submission_id = str(s_t)
            sample_hubmap_id = samp.hubmap_id if len(other_parent_uuids) == 1 else "multiple"
            parent_dataset = None
        else:
            sample_submission_id = sample_hubmap_id = None
            parent_dataset = (
                self.parent_dataset_uuids[0] if len(self.parent_dataset_uuids) == 1 else "multiple"
            )
        return parent_dataset, sample_submission_id, sample_hubmap_id

    def build_rec(self, include_all_children=False):
        """
        Returns a dict containing:

        uuid
        hubmap_id
        status
        group_name
        note

        """
        rec = {
            "uuid": self.uuid,
            "hubmap_id": self.hubmap_id,
            "status": self.status,
            "group_name": self.group_name,
            "note": "",
        }
        rec["note"] = "; ".join([rec["note"]] + self.notes)
        return rec

    def all_uuids(self):
        """
        Returns a list of unique UUIDs associated with this entity
        """
        return super().all_uuids() + self.kid_uuids + self.parent_uuids

    def all_dataset_uuids(self):
        """
        Returns a list of unique dataset or support UUIDs associated with this entity
        """
        return super().all_dataset_uuids() + self.kid_dataset_uuids + self.parent_dataset_uuids


class Sample(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct["entity_type"] == "Sample", (
            f"uuid {prop_dct['uuid']} is" f" a {prop_dct['entity_type']}"
        )
        self.hubmap_id = prop_dct["hubmap_id"]
        self._donor_hubmap_id = None
        self.submission_id = prop_dct["submission_id"]
        self._donor_submission_id = None
        self._donor_uuid = None
        assert "direct_ancestor" in prop_dct, "prop_dct for Sample with no direct_ancestors"
        direct_ancestors_list = [prop_dct["direct_ancestor"]]
        self.parent_uuids = [elt["uuid"] for elt in direct_ancestors_list]
        self._organ = prop_dct.get("organ", None)

    @property
    def organ(self):
        if self._organ is None:
            # There should only be one parent, another Sample
            for parent_uuid in self.parent_uuids:
                parent_entity = self.entity_factory.get(parent_uuid)
                assert isinstance(parent_entity, Sample), "Outermost sample has no organ code?"
                self._organ = parent_entity.organ
        return self._organ

    @property
    def donor_uuid(self):
        if self._donor_uuid is None:
            parent_uuid = self.parent_uuids[0]
            parent_entity = self.entity_factory.get(parent_uuid)
            assert isinstance(parent_entity, (Sample, Donor)), (
                f"Parent of sample {self.uuid}" f"is a {type(parent_entity)}"
            )
            if isinstance(parent_entity, Sample):
                self._donor_uuid = parent_entity.donor_uuid
                self._donor_submission_id = parent_entity.donor_submission_id
                self._donor_hubmap_id = parent_entity.donor_hubmap_id
            else:
                self._donor_uuid = parent_uuid
                self._donor_submission_id = parent_entity.submission_id
                self._donor_hubmap_id = parent_entity.hubmap_id
        return self._donor_uuid

    @property
    def donor_submission_id(self):
        if self._donor_submission_id is None:
            _ = self.donor_uuid  # force search for donor
        return self._donor_submission_id

    @property
    def donor_hubmap_id(self):
        if self._donor_hubmap_id is None:
            _ = self.donor_uuid  # force search for donor
        return self._donor_hubmap_id

    def describe(self, prefix="", file=sys.stdout):
        print(
            f"{prefix}Sample {self.uuid}: "
            f"{self.hubmap_id} "
            f"{self.submission_id} "
            f"{self.donor_submission_id}",
            file=file,
        )


class Donor(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct["entity_type"] == "Donor", (
            f"uuid {prop_dct['uuid']} is" f" a {prop_dct['entity_type']}"
        )
        self.hubmap_id = prop_dct["hubmap_id"]
        self.submission_id = prop_dct["submission_id"]

    def describe(self, prefix="", file=sys.stdout):
        print(
            f"{prefix}Donor {self.uuid}: " f"{self.hubmap_id} " f"{self.submission_id} ", file=file
        )


class Support(Dataset):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct["entity_type"] == "Support", (
            f"uuid {prop_dct['uuid']} is" f" a {prop_dct['entity_type']}"
        )
        self.donor_submission_id = prop_dct["donor"]["submission_id"]

    def describe(self, prefix="", file=sys.stdout):
        print(
            f"{prefix}Support {self.uuid}: "
            f"{self.hubmap_id} "
            f"{self.data_types} "
            f"{self.notes if self.notes else ''}",
            file=file,
        )
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix + "    ", file=file)

    def build_rec(self, include_all_children=False):
        rec = super().build_rec(include_all_children)
        return rec


class EntityFactory:
    def __init__(self, auth_tok=None, instance=None):
        """
        Instance is one of 'PROD', 'STAGE', 'TEST, 'DEV', defaulting to 'PROD'
        """
        assert auth_tok, "auth_tok is required"
        self.auth_tok = auth_tok
        assert instance is None or instance in ["PROD", "STAGE", "TEST", "DEV"], "invalid instance"
        self.instance = instance or "PROD"

    def fetch_new_dataset_table(self):
        """
        Fetches the records provided by the datasets/unpublished endpoint of
        search-api for the current instance, and returns it as a Pandas DataFrame.
        """
        entity_url = ENDPOINTS[self.instance]["entity_url"]
        r = requests.get(
            f"{entity_url}/datasets/unpublished?format=json",
            headers={
                "Authorization": f"Bearer {self.auth_tok}",
                "Content-Type": "application/json",
                "X-Hubmap-Application": "ingest-pipeline",
            },
        )
        if r.status_code >= 300:
            r.raise_for_status()
        df = pd.DataFrame(r.json())  # that's all there is to it!
        return df

    def get(self, uuid):
        """
        Returns an entity of some kind
        """
        if uuid not in ENTITY_CACHE:
            entity_url = ENDPOINTS[self.instance]["entity_url"]
            r = requests.get(
                f"{entity_url}/entities/{uuid}?exclude=direct_ancestors.files",
                headers={
                    "Authorization": f"Bearer {self.auth_tok}",
                    "Content-Type": "application/json",
                },
            )
            if r.status_code >= 300:
                r.raise_for_status()
            prop_dct = r.json()

            assert prop_dct["uuid"] == uuid, (
                f"uuid {uuid} gave back inner" " uuid {prop_dct['uuid']}"
            )
            entity_type = prop_dct["entity_type"]
            if entity_type == "Dataset":
                entity = Dataset(prop_dct, self)
            elif entity_type == "Sample":
                entity = Sample(prop_dct, self)
            elif entity_type == "Donor":
                entity = Donor(prop_dct, self)
            elif entity_type == "Support":
                entity = Support(prop_dct, self)
            elif entity_type == "Upload":
                entity = Upload(prop_dct, self)
            elif entity_type == "Publication":
                entity = Publication(prop_dct, self)
            else:
                entity = Entity(prop_dct, self)

            ENTITY_CACHE[uuid] = entity
        return ENTITY_CACHE[uuid]

    def get_full_path(self, ds_uuid):
        """ds_uuid must be the uuid of a dataset.  Other entity types will fail."""
        ingest_url = ENDPOINTS[self.instance]["ingest_url"]
        r = requests.get(
            f"{ingest_url}/datasets/{ds_uuid}/file-system-abs-path",
            headers={
                "Authorization": f"Bearer {self.auth_tok}",
                "Content-Type": "application/json",
            },
        )
        if r.status_code >= 300:
            r.raise_for_status()
        jsn = r.json()
        assert "path" in jsn, f"could not get file-system-abs-path for {ds_uuid}"
        return Path(jsn["path"])

    def id_to_uuid(self, id_or_uuid):
        """Use the entity service to get the uuid corresponding to an id"""
        entity_url = ENDPOINTS[self.instance]["entity_url"]
        r = requests.get(
            f"{entity_url}/entities/{id_or_uuid}?exclude=direct_ancestors.files",
            headers={
                "Authorization": f"Bearer {self.auth_tok}",
                "Content-Type": "application/json",
            },
        )
        if r.status_code >= 300:
            r.raise_for_status()
        return r.json()["uuid"]

    def create_dataset(
        self,
        provider_info,
        contains_human_genetic_sequences,
        assay_type,
        direct_ancestor_uuids,
        group_uuid,
        description,
        is_epic,
        lab_id,
        priority_project_list=[],
        reindex=True,
    ):
        """
        Creates an entirely new Dataset entity, including updating the databases.
        The created Dataset is returned.  Only a single data_type/assay_type is
        supported.
        """
        ingest_url = ENDPOINTS[self.instance]["ingest_url"]
        data = {
            "provider_info": provider_info,
            "contains_human_genetic_sequences": contains_human_genetic_sequences,
            "dataset_type": assay_type,
            "direct_ancestor_uuids": direct_ancestor_uuids,
            "group_uuid": group_uuid,
            "description": description,
            "lab_id": lab_id,
            "priority_project_list": priority_project_list,
        }
        if is_epic:
            data.update({"creation_action": "External Process"})
        print(f"Creating dataset with data {data}")
        r = requests.post(
            f"{ingest_url}/datasets?reindex={reindex}",
            data=json.dumps(data),
            headers={
                "Authorization": f"Bearer {self.auth_tok}",
                "Content-Type": "application/json",
            },
        )
        if r.status_code >= 300:
            print(f"Error creating dataset {r.json()}")
            r.raise_for_status()
        return r.json()

    def submit_dataset(self, uuid, contains_human_genetic_sequences, reindex=True):
        """
        Takes an existing dataset uuid and submits the dataset.
        """
        ingest_url = ENDPOINTS[self.instance]["ingest_url"]
        data = {"contains_human_genetic_sequences": contains_human_genetic_sequences}
        r = requests.put(
            f"{ingest_url}/datasets/{uuid}/submit?reindex={reindex}",
            data=json.dumps(data),
            headers={
                "Authorization": f"Bearer {self.auth_tok}",
                "Content-Type": "application/json",
            },
        )
        if r.status_code >= 300:
            r.raise_for_status()
        return r.json()


def is_uuid(s):
    return s and len(s) == 32 and all([c in "0123456789abcdef" for c in list(s)])


def robust_find_uuid(s):
    words = s.split("/")
    while words:
        if is_uuid(words[0]):
            return words[0]
        words = words[1:]
    raise RuntimeError(f"No uuid found in {s}")


def get_uuid(rec):
    return robust_find_uuid(rec["data_path"])


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "metadatatsv", help="input .tsv or .xlsx file, or a list of uuids in a .txt file"
    )
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
    parser.add_argument(
        "--include_all_children",
        action="store_true",
        help="include all children, not just those in the QA or Published states",
    )
    args = parser.parse_args()
    auth_tok = input("auth_tok: ")
    entity_factory = EntityFactory(auth_tok)
    if args.metadatatsv.endswith(".tsv"):
        in_df = pd.read_csv(args.metadatatsv, sep="\t")
    elif args.metadatatsv.endswith(".xlsx"):
        in_df = pd.read_excel(args.metadatatsv)
    elif args.metadatatsv.endswith(".txt"):
        # a list of bare uuids
        recs = []
        for line in open(args.metadatatsv):
            assert is_uuid(line.strip()), (
                f"text file {args.metadatatsv} contains" " non-uuid {line.strip}"
            )
            recs.append({"data_path": line.strip()})
        in_df = pd.DataFrame(recs)
    else:
        raise RuntimeError("Unrecognized input file format")
    in_df["uuid"] = in_df.apply(get_uuid, axis=1)
    out_recs = []

    known_uuids = set()
    for _, row in in_df.iterrows():
        uuid = row["uuid"]
        ds = entity_factory.get(uuid)
        ds.describe()
        new_uuids = ds.all_dataset_uuids()
        rec = {}
        if hasattr(ds, "build_rec"):
            try:
                rec = ds.build_rec(include_all_children=args.include_all_children)
                if any([uuid in known_uuids for uuid in new_uuids]):
                    old_note = rec["note"] if "note" in rec else ""
                    rec["note"] = "UUID COLLISION! " + old_note
                known_uuids = known_uuids.union(new_uuids)
            except AssertionError as e:
                old_note = rec["note"] if "note" in rec else ""
                rec["note"] = f"BAD UUID: {e} " + old_note
                rec["uuid"] = uuid  # just to make sure it is present
        if rec:
            out_recs.append(rec)
    out_df = pd.DataFrame(out_recs).rename(
        columns={
            "qa_child_uuid": "derived_uuid",
            "child_uuid": "derived_uuid",
            "qa_child_hubmap_id": "derived_hubmap_id",
            "child_hubmap_id": "derived_hubmap_id",
            "qa_child_data_type": "derived_data_type",
            "child_data_type": "derived_data_type",
            "qa_child_status": "derived_status",
            "child_status": "derived_status",
        }
    )
    out_df = out_df.sort_values([key for key in ROW_SORT_KEYS if key in out_df.columns], axis=0)
    out_df.to_csv(
        args.out, sep="\t", index=False, columns=column_sorter(elt for elt in out_df.columns)
    )


if __name__ == "__main__":
    main()
