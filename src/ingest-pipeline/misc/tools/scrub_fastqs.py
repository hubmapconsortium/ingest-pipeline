import hashlib
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SRA_SCRUBBER_IMAGE = "ncbi/sra-human-scrubber:2.2.1"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_scrubber(input_path: Path, output_name: str) -> None:
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

    t0 = time.time()
    _run_scrubber(fastq_path, clean.name)
    print(f"[scrub] {fastq_path.name}: first scrub took {time.time() - t0:.2f}s")
    t0 = time.time()
    _run_scrubber(clean, clean_clean.name)
    print(f"[scrub] {fastq_path.name}: second scrub took {time.time() - t0:.2f}s")

    if _sha256(clean) != _sha256(clean_clean):
        raise RuntimeError(
            f"Idempotency check failed for {fastq_path}: "
            "second scrubber pass produced different output than its input"
        )

    fastq_path.rename(fastq_path.parent / (fastq_path.name + ".original"))
    clean.rename(fastq_path)
    clean_clean.unlink()


def _scrub_fastq_gz(fastq_gz_path: Path, num_threads: int = 1) -> None:
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

    t0 = time.time()
    with open(fastq_path, "wb") as f_out:
        subprocess.run(
            ["pigz", "-d", "-p", str(num_threads), "-c", str(fastq_gz_path)],
            stdout=f_out,
            check=True,
        )
    print(f"[scrub] {fastq_gz_path.name}: decompression took {time.time() - t0:.2f}s")

    t0 = time.time()
    _scrub_fastq(fastq_path, num_threads)
    print(f"[scrub] {fastq_gz_path.name}: full docker scrub took {time.time() - t0:.2f}s")

    fastq_gz_path.rename(parent / (fastq_gz_path.name + ".original"))

    t0 = time.time()
    with open(fastq_gz_path, "wb") as f_out:
        subprocess.run(
            ["pigz", "-p", str(num_threads), "-c", str(fastq_path)],
            stdout=f_out,
            check=True,
        )
    print(f"[scrub] {fastq_gz_path.name}: recompression took {time.time() - t0:.2f}s")

    # Delete uncompressed files
    fastq_path.unlink()
    fastq_original.unlink()


def _run_in_pool(fns_and_args, num_threads: int) -> None:
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(fn, *args): args for fn, *args in fns_and_args}
        errors = []
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                errors.append(exc)
        if errors:
            raise RuntimeError(f"{len(errors)} scrub job(s) failed: {errors}")


def scrub_upload(upload_path: Path, num_threads: int = 1) -> None:
    """
    Finds all FASTQ files under upload_path, runs the SRA scrubber twice per file,
    validates idempotency via SHA-256 hash comparison, and renames files in place.
    Raises on any failure.

    Processes .fastq.gz files before .fastq files to avoid double-processing
    files decompressed from a previous partial run. Any .fastq file that has a
    corresponding .fastq.gz or .fastq.gz.original in the same directory is skipped,
    as it is a decompressed intermediate from the .gz pass.
    """
    _run_in_pool(
        [(_scrub_fastq_gz, p, num_threads) for p in sorted(upload_path.rglob("*.fastq.gz"))],
        num_threads,
    )

    plain_fastqs = []
    for fastq in sorted(upload_path.rglob("*.fastq")):
        parent = fastq.parent
        stem = fastq.name[: -len(".fastq")]
        if (parent / (stem + ".fastq.gz")).exists() or (
            parent / (stem + ".fastq.gz.original")
        ).exists():
            continue
        plain_fastqs.append((_scrub_fastq, fastq, num_threads))
    _run_in_pool(plain_fastqs, num_threads)

    for orig_fastq in sorted(upload_path.rglob("*.fastq.*.original")):
        orig_fastq.unlink()
