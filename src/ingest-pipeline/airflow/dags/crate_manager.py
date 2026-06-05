import logging
from pathlib import Path
from itertools import count


def find_most_recent_incarnation() -> dict | None :
    return None


class CrateManager():
    instance_counter = count()

    def __init__(self):
        self.tmp_dir = None
        self.crate_dir = None
        self.crate_counter = 0
        self.instance = next(self.instance_counter)
        logging.debug(f"CrateManager initialized instance {self.instance}")
        print(f"CrateManager initialized instance {self.instance}")

    def check_scratch_dir(self, tmp_dir: [Path, str]) -> None:
        if self.tmp_dir is None:
            self.tmp_dir = Path(tmp_dir)
            self.crate_dir = self.tmp_dir / "crates"
            self.crate_dir.mkdir(exist_ok=True)
            logging.debug(f"CrateManager {self.instance}: tmp_dir set to {self.tmp_dir}")
            print(f"CrateManager {self.instance}: tmp_dir set to {self.tmp_dir}")
        elif self.tmp_dir != Path(tmp_dir):
            raise RuntimeError(f"CrateManager: tmp_dir changed from {self.tmp_dir} to {tmp_dir}")

    def _next_crate_dir(self) -> Path:
        rslt = self.crate_dir / f"crate_{self.crate_counter:03d}"
        rslt.mkdir()
        self.crate_counter += 1
        return rslt

    def get_args(self, tmp_dir: [Path, str], ti, session) -> list[str]:
        self.check_scratch_dir(tmp_dir)
        logging.debug(f"ti is a {type(ti)}")
        print(f"ti is a {type(ti)}")
        logging.debug(f"session is a {type(session)}")
        print(f"session is a {type(session)}")
        if self.crate_dir is None:
            raise RuntimeError("CrateManager: tmp_dir not set")
        assert self.crate_counter < 1000, "CrateManager: too many crates created"
        rslt = ["--debug", "--provenance", f"{self._next_crate_dir()}"]
        rslt = ["--debug"]
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
