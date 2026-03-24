import gzip
import hashlib
import shutil
import subprocess
from pathlib import Path

SRA_SCRUBBER_IMAGE = "ncbi/sra-human-scrubber:2.2.1"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_scrubber(input_path: Path, output_name: str) -> None:
    # Invoke docker container and pass data
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{input_path.parent}:/data",
            SRA_SCRUBBER_IMAGE,
            "scrub.sh",
            "-i",
            f"/data/{input_path.name}",
            "-o",
            f"/data/{output_name}",
        ],
        check=True,
    )


def _scrub_fastq(fastq_path: Path) -> None:
    """
    Process a single uncompressed FASTQ file in place.

    On success:
      - fastq_path        → fastq_path + ".original"  (audit trail)
      - fastq_path.clean  → fastq_path                (cleaned file takes original name)
    """
    clean = fastq_path.parent / (fastq_path.name + ".clean")
    clean_clean = fastq_path.parent / (fastq_path.name + ".clean.clean")

    _run_scrubber(fastq_path, clean.name)
    _run_scrubber(clean, clean_clean.name)

    if _sha256(clean) != _sha256(clean_clean):
        raise RuntimeError(
            f"Idempotency check failed for {fastq_path}: "
            "second scrubber pass produced different output than its input"
        )

    fastq_path.rename(fastq_path.parent / (fastq_path.name + ".original"))
    clean.rename(fastq_path)
    clean_clean.unlink()


def _scrub_fastq_gz(fastq_gz_path: Path) -> None:
    """
    Process a single gzipped FASTQ file in place.

    The original .fastq.gz remains untouched until the idempotency check passes,
    so a failure before the rename step leaves the upload fully recoverable.

    On success:
      - fastq_gz_path         → fastq_gz_path + ".original"  (audit trail)
      - recompressed clean    → fastq_gz_path                (cleaned file takes original name)
    """
    parent = fastq_gz_path.parent
    # Derive stem: "sample" from "sample.fastq.gz"
    stem = fastq_gz_path.name[: -len(".fastq.gz")]
    fastq_path = parent / (stem + ".fastq")
    fastq_original = parent / (stem + ".fastq.original")

    # Uncompress file
    with gzip.open(fastq_gz_path, "rb") as f_in, open(fastq_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    # After decompression, run fastq through regular scrub routine
    _scrub_fastq(fastq_path)

    # Scrubbing will move original fastq to ".original" and the clean file to fastq

    # Rename original .fastq.gz
    fastq_gz_path.rename(parent / (fastq_gz_path.name + ".original"))

    # Recompress clean output to original name
    with open(fastq_path, "rb") as f_in, gzip.open(fastq_gz_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    # Delete uncompressed files
    fastq_path.unlink()
    fastq_original.unlink()


def scrub_upload(upload_path: Path) -> None:
    """
    Finds all FASTQ files under upload_path, runs the SRA scrubber twice per file,
    validates idempotency via SHA-256 hash comparison, and renames files in place.
    Raises on any failure.

    Processes .fastq.gz files before .fastq files to avoid double-processing
    files decompressed from a previous partial run. Any .fastq file that has a
    corresponding .fastq.gz or .fastq.gz.original in the same directory is skipped,
    as it is a decompressed intermediate from the .gz pass.
    """
    for fastq_gz in sorted(upload_path.rglob("*.fastq.gz")):
        _scrub_fastq_gz(fastq_gz)
    for fastq in sorted(upload_path.rglob("*.fastq")):
        parent = fastq.parent
        stem = fastq.name[: -len(".fastq")]
        # Ignore any fastqs that might exist that also have a compressed version.
        if (parent / (stem + ".fastq.gz")).exists() or (
            parent / (stem + ".fastq.gz.original")
        ).exists():
            continue
        _scrub_fastq(fastq)

    # Finally, delete these originals.
    for orig_fastq in sorted(upload_path.rglob("*.fastq.*.original")):
        orig_fastq.unlink()
