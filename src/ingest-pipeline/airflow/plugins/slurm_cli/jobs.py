from dataclasses import dataclass


@dataclass
class SlurmJobStatus:
    job_id: str
    job_name: str
    status_code: str
    status: str
    reason: str
