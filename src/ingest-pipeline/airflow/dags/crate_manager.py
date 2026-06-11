import logging
from pathlib import Path
from itertools import count
from pprint import pprint
from airflow.models import DagRun, XCom

CRATE_STATE_KEY = "crate_state"


def find_most_recent_incarnation(ti, session) -> dict | None :
    query = (
        session.query(XCom).join(DagRun,
                                 (XCom.dag_id == DagRun.dag_id)
                                 & (XCom.run_id == DagRun.run_id))
        .filter(XCom.dag_id == ti.dag_id)
        .filter(XCom.key == CRATE_STATE_KEY)
        .filter(DagRun.run_id == ti.run_id)
        .order_by(DagRun.execution_date.desc())
        )
    rec = query.first()
    return rec.value if rec else None


class CrateManager():
    instance_counter = count()

    def __init__(self):
        self.tmp_dir = None
        self.crate_dir = None
        self.crate_counter = 0
        self.crate_chain = []
        self.instance = next(self.instance_counter)
        logging.debug(f"CrateManager initialized instance {self.instance}")
        print(f"CrateManager initialized instance {self.instance}")

    def to_json(self)-> dict:
        return {
            "instance": self.instance,
            "crate_counter": self.crate_counter,
            "crate_chain": self.crate_chain,
        }

    def update_from_json(self, blob: dict) -> None:
        assert blob["instance"] == self.instance
        self.crate_counter = blob["crate_counter"]
        self.crate_chain = blob["crate_chain"]
        

    def check_scratch_dir(self, tmp_dir: [Path, str]) -> None:
        if self.tmp_dir is None:
            self.tmp_dir = Path(tmp_dir)
            self.crate_dir = self.tmp_dir / "crates"
            self.crate_dir.mkdir(exist_ok=True)
            logging.debug(f"CrateManager {self.instance}: tmp_dir set to {self.tmp_dir}")
            print(f"CrateManager {self.instance}: tmp_dir set to {self.tmp_dir}")
        elif self.tmp_dir != Path(tmp_dir):
            raise RuntimeError(f"CrateManager: tmp_dir changed from {self.tmp_dir} to {tmp_dir}")

    def create_crate_dir(self) -> Path:
        rslt = self.crate_dir / f"crate_{self.crate_counter:03d}"
        rslt.mkdir()
        return rslt

    def get_args(self, tmp_dir: [Path, str], ti, session) -> list[str]:
        self.check_scratch_dir(tmp_dir)
        if self.crate_dir is None:
            raise RuntimeError("CrateManager: tmp_dir not set")
        assert self.crate_counter < 1000, "CrateManager: too many crates created"
        if prev_info := find_most_recent_incarnation(ti, session):
            self.update_from_json(prev_info)
            logging.debug(f"updated from previous incarnation {prev_info}")
            self.crate_counter += 1
        else:
            logging.debug("no previous incarnation found")
        crate_path = self.create_crate_dir()
        self.crate_chain.append(str(crate_path))
        if self.crate_counter > 0:
            rslt = ["--provenance", f"{crate_path}"]
        else:
            rslt = []
        ti.xcom_push(key=CRATE_STATE_KEY, value=self.to_json())
        return rslt
    
    def get_build_crate_cmd(self) -> str:
        assert self.tmp_dir is not None, "CrateManager: tmp_dir not set"
        return f"echo 'crate_dir for CrateManager {self.instance} is {self.crate_dir}'"


class DummyCrateManager(CrateManager):
    def __init__(self):
        super().__init__()
        logging.debug(f"DummyCrateManager {self.instance} initialized")
        print(f"DummyCrateManager {self.instance} initialized")

    def check_scratch_dir(self, tmp_dir: [Path, str]) -> None:
        logging.debug(f"DummyCrateManager {self.instance}: check_scratch_dir called with {tmp_dir}")
        print(f"DummyCrateManager {self.instance}: check_scratch_dir called with {tmp_dir}")

    def get_args(self, tmp_dir: [Path, str]) -> list[str]:
        logging.debug("DummyCrateManager {self.instance}: get_args called")
        print("DummyCrateManager {self.instance}: get_args called")
        return []

    def get_build_crate_cmd(self) -> str:
        logging.debug("DummyCrateManager {self.instance}: get_build_crate_cmd called")
        print("DummyCrateManager {self.instance}: get_build_crate_cmd called")
        return "echo 'this is a dummy crate manager'"
