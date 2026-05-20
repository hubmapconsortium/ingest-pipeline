import hashlib
import shutil
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
    # Mount a per-file tmp dir from the NFS into the container so that any
    # files the scrubber writes outside /data (e.g. /tmp) land on NFS, not
    # on local disk.
    container_tmp = input_path.parent / f".scrub_tmp_{input_path.name}"
    container_tmp.mkdir(exist_ok=True)
    try:
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{input_path.parent}:/data",
                "-v",
                f"{container_tmp}:/tmp",
                SRA_SCRUBBER_IMAGE,
                "scrub.sh",
                "-i",
                f"/data/{input_path.name}",
                "-o",
                f"/data/{output_name}",
            ],
            check=True,
        )
    finally:
        shutil.rmtree(container_tmp, ignore_errors=True)


def _double_scrub(fastq_path: Path) -> None:
    """
    Run the scrubber twice on fastq_path, verify idempotency via SHA-256,
    then replace fastq_path with the scrubbed result.
    Cleans up intermediate files on success or failure.
    fastq_path is left untouched on failure.
    """
    clean = fastq_path.parent / (fastq_path.name + ".clean")
    clean_clean = fastq_path.parent / (fastq_path.name + ".clean.clean")
    try:
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

        clean.replace(fastq_path)
        clean_clean.unlink()
    except Exception:
        print(f"[scrub] ERROR scrubbing {fastq_path.name}, rolling back")
        clean.unlink(missing_ok=True)
        clean_clean.unlink(missing_ok=True)
        raise


def _process_fastq_gz(fastq_gz_path: Path, num_threads: int) -> None:
    """
    Process a single .fastq.gz file in place.

    Steps:
      1. Copy .fastq.gz → .fastq.gz.original  (backup)
      2. Decompress to .fastq
      3. Double-scrub the .fastq
      4. Recompress, overwriting .fastq.gz
      5. Delete the .fastq intermediate

    On failure:
      - .fastq intermediate deleted
      - If .fastq.gz was overwritten, restored from .original; otherwise .original deleted
      - Exception re-raised

    On success: .fastq.gz has scrubbed data; .fastq.gz.original retains the original.
    """
    parent = fastq_gz_path.parent
    stem = fastq_gz_path.name[: -len(".fastq.gz")]
    fastq_path = parent / (stem + ".fastq")
    original = parent / (fastq_gz_path.name + ".original")

    gz_overwritten = False
    try:
        shutil.copy2(fastq_gz_path, original)

        t0 = time.time()
        with open(fastq_path, "wb") as f_out:
            subprocess.run(
                ["pigz", "-d", "-p", str(num_threads), "-c", str(fastq_gz_path)],
                stdout=f_out,
                check=True,
            )
        print(f"[scrub] {fastq_gz_path.name}: decompression took {time.time() - t0:.2f}s")

        t0 = time.time()
        _double_scrub(fastq_path)
        print(f"[scrub] {fastq_gz_path.name}: scrubbing took {time.time() - t0:.2f}s")

        gz_overwritten = True
        t0 = time.time()
        with open(fastq_gz_path, "wb") as f_out:
            subprocess.run(
                ["pigz", "-p", str(num_threads), "-c", str(fastq_path)],
                stdout=f_out,
                check=True,
            )
        print(f"[scrub] {fastq_gz_path.name}: recompression took {time.time() - t0:.2f}s")

        fastq_path.unlink()
    except Exception:
        print(f"[scrub] ERROR processing {fastq_gz_path.name}, cleaning up")
        fastq_path.unlink(missing_ok=True)
        if gz_overwritten:
            original.replace(fastq_gz_path)
        else:
            original.unlink(missing_ok=True)
        raise


def _process_fastq(fastq_path: Path) -> None:
    """
    Process a single .fastq file in place.

    Steps:
      1. Copy .fastq → .fastq.original  (backup)
      2. Double-scrub the .fastq

    On failure: .fastq restored from .original; exception re-raised.
    On success: .fastq has scrubbed data; .fastq.original retains the original.
    """
    original = fastq_path.parent / (fastq_path.name + ".original")
    copy_done = False
    try:
        shutil.copy2(fastq_path, original)
        copy_done = True

        t0 = time.time()
        _double_scrub(fastq_path)
        print(f"[scrub] {fastq_path.name}: scrubbing took {time.time() - t0:.2f}s")
    except Exception:
        print(f"[scrub] ERROR processing {fastq_path.name}, cleaning up")
        if copy_done:
            # backup is intact — restore it
            original.replace(fastq_path)
        else:
            # copy failed — fastq_path is still good, discard partial backup
            original.unlink(missing_ok=True)
        raise


def _run_in_pool(tasks: list, num_threads: int) -> list[Exception]:
    errors = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(fn, *args): args for fn, *args in tasks}
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                errors.append(exc)
                for f in futures:
                    f.cancel()
    return errors


def _rollback_all(files: list[Path]) -> None:
    """
    Restore .original backups for any file that completed successfully.
    Files that failed individually already rolled back themselves (no .original remains).
    """
    for path in files:
        original = path.parent / (path.name + ".original")
        if original.exists():
            original.replace(path)
            print(f"[scrub] Rolled back {path.name}")


def scrub_upload(upload_path: Path, num_threads: int = 1) -> None:
    """
    Find all FASTQ files under upload_path, scrub human reads from each,
    verify correctness, then remove .original backups. Raises on any failure,
    rolling back all changes so every file is restored to its pre-scrub state.

    Raises RuntimeError with per-file error details on failure.
    """
    # Step 1: Discover files and check for conflicts
    gz_files = sorted(upload_path.rglob("*.fastq.gz"))
    all_fastqs = sorted(upload_path.rglob("*.fastq"))

    gz_keys = {(p.parent, p.name[: -len(".fastq.gz")] + ".fastq") for p in gz_files}
    conflicts = [f for f in all_fastqs if (f.parent, f.name) in gz_keys]
    if conflicts:
        raise RuntimeError(
            "Found .fastq files with a matching .fastq.gz in the same directory "
            f"(resolve before scrubbing): {[str(f) for f in conflicts]}"
        )

    plain_fastqs = [f for f in all_fastqs if (f.parent, f.name) not in gz_keys]

    # Step 2: Scrub .fastq.gz files
    gz_errors = _run_in_pool(
        [(_process_fastq_gz, p, num_threads) for p in gz_files],
        num_threads,
    )
    if gz_errors:
        print(f"[scrub] {len(gz_errors)} .fastq.gz job(s) failed — rolling back all gz files")
        _rollback_all(gz_files)
        raise RuntimeError(
            f"{len(gz_errors)} .fastq.gz scrub job(s) failed:\n"
            + "\n".join(str(e) for e in gz_errors)
        )

    # Step 3: Scrub plain .fastq files
    fastq_errors = _run_in_pool(
        [(_process_fastq, p) for p in plain_fastqs],
        num_threads,
    )
    if fastq_errors:
        print(f"[scrub] {len(fastq_errors)} .fastq job(s) failed — rolling back all files")
        _rollback_all(gz_files)
        _rollback_all(plain_fastqs)
        raise RuntimeError(
            f"{len(fastq_errors)} .fastq scrub job(s) failed:\n"
            + "\n".join(str(e) for e in fastq_errors)
        )

    # Step 4a: Verify every discovered file has both a scrubbed copy and a .original backup
    missing = []
    for path in gz_files + plain_fastqs:
        original = path.parent / (path.name + ".original")
        if not path.exists():
            missing.append(str(path))
        if not original.exists():
            missing.append(str(original))
    if missing:
        raise RuntimeError(
            "Post-scrub verification failed — expected files not found:\n"
            + "\n".join(missing)
        )

    # Step 4b: All files verified — delete .original backups
    for path in gz_files + plain_fastqs:
        (path.parent / (path.name + ".original")).unlink()