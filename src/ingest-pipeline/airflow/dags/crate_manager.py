import logging
from pathlib import Path

class CrateManager():
    def __init__(self):
        self.tmp_dir = None
        self.crate_dir = None
        self.crate_counter = 0
        logging.debug("CrateManager initialized")

    def check_scratch_dir(self, tmp_dir: [Path, str]) -> None:
        if self.tmp_dir is None:
            self.tmp_dir = Path(tmp_dir)
            self.crate_dir = self.tmp_dir / "crates"
            logging.debug(f"CrateManager: tmp_dir set to {self.tmp_dir}")
        elif self.tmp_dir != Path(tmp_dir):
            raise RuntimeError(f"CrateManager: tmp_dir changed from {self.tmp_dir} to {tmp_dir}")

    def get_args(self, tmp_dir: [Path, str]) -> list[str]:
        self.check_scratch_dir(tmp_dir)
        if self.crate_dir is None:
            raise RuntimeError("CrateManager: tmp_dir not set")
        assert self.crate_counter < 1000, "CrateManager: too many crates created"
        rslt = ["--provenance", f"{self.crate_dir}/crate_{self.crate_counter:03d}"]
        self.crate_counter += 1
        return rslt
    
    def get_build_crate_cmd(self) -> str:
        assert self.tmp_dir is not None, "CrateManager: tmp_dir not set"
        return f"echo 'crate_dir is {self.crate_dir}'"


class DummyCrateManager(CrateManager):
    def __init__(self):
        super().__init__()
        logging.debug("DummyCrateManager initialized")

    def check_scratch_dir(self, tmp_dir: [Path, str]) -> None:
        logging.debug(f"DummyCrateManager: check_scratch_dir called with {tmp_dir}")

    def get_args(self, tmp_dir: [Path, str]) -> list[str]:
        logging.debug("DummyCrateManager: get_args called")
        return []

    def get_build_crate_cmd(self) -> str:
        logging.debug("DummyCrateManager: get_build_crate_cmd called")
        return "echo 'this is a dummy crate manager'"