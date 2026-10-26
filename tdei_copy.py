#!/usr/bin/env python3
"""Copies all datasets between TDEI project groups.

We can move datasets between different TDEI environments. The process downloads
each dataset and its metadata from the source before uploading them to the
target, where they arrive with Pre-Release status.

Since dataset names and versions must be unique within an environment, we skip
any that already exist in the target group. If an upload job for a specific
name and version is still running in the target group, we report it as
in-progress and avoid a second upload. Any other conflicts, including names
claimed by different groups, result in failures.

Each upload requires a target project group service. We can pair specific
source service IDs with target ones using `--service-map`, or use
`--default-service` to specify a fallback target service for all unmatched
datasets of a given data type. Without these flags, we map each dataset to a
target service that shares its data type and name.

We obtain passwords through TDEI_SOURCE_PASSWORD and TDEI_TARGET_PASSWORD
environment variables or via interactive prompts. The target account defaults
to whatever we use for the source account.

Examples:

    uv run tdei-copy --source-env stage --source-group <uuid> \\
        --target-env prod --target-group <uuid> --source-user me@example.com

    uv run tdei-copy --source-url https://tdei.example.org --source-group <uuid> \\
        --target-env dev --target-group <uuid> --source-user me@example.com \\
        --default-service osw=<target-service-uuid> --dry-run
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
from tqdm import tqdm

ENVIRONMENTS = {
    "dev": "https://api-dev.tdei.us",
    "stage": "https://api-stage.tdei.us",
    "prod": "https://api.tdei.us",
}

# We map dataset list filter values to the path segments we need for the
# download and upload APIs.
DATA_TYPES = {
    "osw": "osw",
    "flex": "gtfs-flex",
    "pathways": "gtfs-pathways",
}

PAGE_SIZE = 50
JOB_TERMINAL_STATES = {"COMPLETED", "FAILED", "ABANDONED"}


class TdeiError(Exception):
    pass


class TdeiClient:
    """Provides authenticated access to a single TDEI environment.

    When a request yields a 401 error, the client performs a single
    re-authentication and retry attempt, which ensures long copies continue
    after access tokens expire.
    """

    def __init__(self, base_url: str, username: str, password: str, timeout: float):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self._password = password
        self.timeout = timeout
        self.session = requests.Session()

    def authenticate(self) -> None:
        resp = requests.post(
            f"{self.base_url}/api/v1/authenticate",
            json={"username": self.username, "password": self._password},
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise TdeiError(
                f"unable to authenticate {self.username} via {self.base_url}: HTTP {resp.status_code} {resp.text.strip()[:200]}"
            )
        token = resp.json()["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        url = f"{self.base_url}{path}"
        resp = self.session.request(method, url, **kwargs)
        if resp.status_code == 401:
            resp.close()
            self.authenticate()
            resp = self.session.request(method, url, **kwargs)
        if not resp.ok:
            detail = resp.text.strip()[:500] if not kwargs.get("stream") else ""
            raise TdeiError(f"{method} {path} encountered error: HTTP {resp.status_code} {detail}")
        return resp

    def paginate(self, path: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        page = 1
        while True:
            items = self.request("GET", path, params={**params, "page_no": page, "page_size": PAGE_SIZE}).json()
            yield from items
            if len(items) < PAGE_SIZE:
                return
            page += 1

    def list_datasets(self, project_group_id: str, data_type: str, status: str) -> list[dict]:
        return list(
            self.paginate(
                "/api/v1/datasets",
                {"tdei_project_group_id": project_group_id, "data_type": data_type, "status": status},
            )
        )

    def list_services(self, project_group_id: str) -> list[dict]:
        return list(self.paginate("/api/v1/services", {"tdei_project_group_id": project_group_id}))

    def download(self, data_type: str, dataset_id: str, dest: Path, label: str) -> int:
        path = f"/api/v1/{DATA_TYPES[data_type]}/{dataset_id}"
        with self.request("GET", path, stream=True) as resp:
            total = int(resp.headers.get("Content-Length", 0)) or None
            with (
                open(dest, "wb") as fh,
                tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"  download {label}",
                    leave=False,
                ) as bar,
            ):
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
                    bar.update(len(chunk))
        return dest.stat().st_size

    def upload(
        self,
        data_type: str,
        project_group_id: str,
        service_id: str,
        dataset_file: Path,
        metadata_file: Path,
        changeset_file: Path | None,
        label: str,
    ) -> str:
        """Uploads a dataset to the TDEI API and returns the upload request job
        ID.
        """
        path = f"/api/v1/{DATA_TYPES[data_type]}/upload/{project_group_id}/{service_id}"
        for attempt in range(2):
            with ExitStack() as stack:
                fields = {
                    "dataset": (dataset_file.name, stack.enter_context(open(dataset_file, "rb")), "application/zip"),
                    "metadata": ("metadata.json", stack.enter_context(open(metadata_file, "rb")), "application/json"),
                }
                if changeset_file:
                    fields["changeset"] = (
                        changeset_file.name,
                        stack.enter_context(open(changeset_file, "rb")),
                        "application/octet-stream",
                    )
                encoder = MultipartEncoder(fields=fields)
                with tqdm(
                    total=encoder.len,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"  upload   {label}",
                    leave=False,
                ) as bar:
                    seen = 0

                    def on_read(monitor: MultipartEncoderMonitor) -> None:
                        nonlocal seen
                        bar.update(monitor.bytes_read - seen)
                        seen = monitor.bytes_read

                    monitor = MultipartEncoderMonitor(encoder, on_read)
                    resp = self.session.post(
                        f"{self.base_url}{path}",
                        data=monitor,
                        headers={"Content-Type": monitor.content_type},
                        timeout=self.timeout,
                    )
            if resp.status_code == 401 and attempt == 0:
                self.authenticate()
                continue
            if not resp.ok:
                raise TdeiError(f"upload failed: HTTP {resp.status_code} {resp.text.strip()[:500]}")
            return resp.text.strip().strip('"')
        raise TdeiError("could not upload: authentication failed after retry")

    def running_uploads(self, project_group_id: str) -> dict[tuple[str, str], str]:
        """Maps pairs of dataset names and versions to their corresponding job IDs
        for upload tasks with an IN-PROGRESS status.
        """
        jobs = self.paginate(
            "/api/v1/jobs",
            {
                "tdei_project_group_id": project_group_id,
                "job_type": "Dataset-Upload",
                "status": "IN-PROGRESS",
                "show_group_jobs": "true",
            },
        )
        return {
            (job["request_input"].get("dataset_name", ""), str(job["request_input"].get("dataset_version", ""))): str(job["job_id"])
            for job in jobs
            if job.get("request_input")
        }

    def get_job(self, job_id: str, project_group_id: str) -> dict:
        jobs = self.request(
            "GET",
            "/api/v1/jobs",
            params={"job_id": job_id, "tdei_project_group_id": project_group_id},
        ).json()
        if not jobs:
            raise TdeiError(f"job {job_id} not found")
        return jobs[0]

    def wait_for_job(
        self,
        job_id: str,
        project_group_id: str,
        timeout: float,
        interval: float,
        label: str,
    ) -> dict:
        deadline = time.monotonic() + timeout
        with tqdm(desc=f"  process  {label}", total=1, leave=False, bar_format="{desc} {bar} {n}/{total} {postfix}") as bar:
            while True:
                job = self.get_job(job_id, project_group_id)
                progress = job.get("progress") or {}
                bar.total = progress.get("total_stages") or bar.total
                bar.n = progress.get("completed_stages") or 0
                bar.set_postfix_str(progress.get("current_stage") or job.get("current_stage") or "")
                if job.get("status") in JOB_TERMINAL_STATES:
                    return job
                if time.monotonic() > deadline:
                    raise TdeiError(f"job {job_id} remains {job.get('status')} following {timeout:.0f}s")
                time.sleep(interval)


@dataclass
class CopyResult:
    source_id: str
    name: str
    version: str
    data_type: str
    source_service: str
    status: str = "pending"  # we use copied, uploaded, in-progress, failed, skipped, or planned
    target_service_id: str | None = None
    job_id: str | None = None
    target_dataset_id: str | None = None
    size_bytes: int | None = None
    error: str | None = None
    elapsed_s: float = 0.0


def resolve_base_url(env: str | None, url: str | None, side: str) -> str:
    if url:
        return url
    if env:
        return ENVIRONMENTS[env]
    raise SystemExit(f"fail: please provide either --{side}-env or --{side}-url")


def parse_pairs(values: list[str], flag: str) -> dict[str, str]:
    pairs = {}
    for value in values:
        key, sep, val = value.partition("=")
        if not sep or not key or not val:
            raise SystemExit(f"invalid format for {flag}: anticipated KEY=VALUE, received {value!r}")
        pairs[key.strip()] = val.strip()
    return pairs


def get_password(env_var: str, prompt: str) -> str:
    return os.environ.get(env_var) or getpass.getpass(prompt)


def safe_filename(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_")[:80] or "dataset"


def pick_target_service(
    dataset: dict,
    data_type: str,
    target_services: list[dict],
    service_map: dict[str, str],
    default_services: dict[str, str],
) -> str | None:
    source_service = dataset.get("service") or {}
    if source_service.get("tdei_service_id") in service_map:
        return service_map[source_service["tdei_service_id"]]
    name = source_service.get("name")
    for svc in target_services:
        if svc.get("service_name") == name and svc.get("service_type") == data_type:
            return svc["tdei_service_id"]
    return default_services.get(data_type)


def unwrap_download(download: Path, workdir: Path, stem: str) -> tuple[Path, Path | None]:
    """Splits a downloaded dataset into files compatible with the upload API.

    A wrapper zip contains the dataset zip, a `metadata.json`, and may include
    a changeset (`.osc` or `.zip`). The function returns the dataset zip along
    with any changeset found. If loose data files exist instead of a dataset
    zip, these files get repacked into a new zip.
    """
    if not zipfile.is_zipfile(download):
        raise TdeiError("the retrieved file isn't a valid zip archive")
    with zipfile.ZipFile(download) as outer:
        names = [n for n in outer.namelist() if not n.endswith("/")]
        changesets = [n for n in names if "changeset" in Path(n).name.lower() and n.lower().endswith((".osc", ".zip"))]
        data = [n for n in names if n not in changesets and Path(n).name != "metadata.json"]
        inner_zips = [n for n in data if n.lower().endswith(".zip")]
        if len(inner_zips) > 1:
            raise TdeiError(f"download holds multiple zip files, cannot select the dataset: {inner_zips}")
        if len(changesets) > 1:
            raise TdeiError(f"multiple changesets found in download: {changesets}")

        dataset_file = workdir / f"{stem}.dataset.zip"
        if inner_zips:
            with outer.open(inner_zips[0]) as src, open(dataset_file, "wb") as dst:
                shutil.copyfileobj(src, dst)
        elif data:
            with zipfile.ZipFile(dataset_file, "w", zipfile.ZIP_DEFLATED) as repacked:
                for name in data:
                    repacked.writestr(Path(name).name, outer.read(name))
        else:
            raise TdeiError("the download contains zero dataset files")

        changeset_file = None
        if changesets:
            changeset_file = workdir / f"{stem}.changeset{Path(changesets[0]).suffix}"
            with outer.open(changesets[0]) as src, open(changeset_file, "wb") as dst:
                shutil.copyfileobj(src, dst)
    return dataset_file, changeset_file


def dataset_key(dataset: dict) -> tuple[str, str]:
    detail = (dataset.get("metadata") or {}).get("dataset_detail") or {}
    return detail.get("name", ""), str(detail.get("version", ""))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    for side in ("source", "target"):
        group = parser.add_argument_group(f"{side}")
        where = group.add_mutually_exclusive_group()
        where.add_argument(f"--{side}-env", choices=sorted(ENVIRONMENTS), help=f"{side} TDEI environment")
        where.add_argument(f"--{side}-url", help=f"{side} API base URL for custom environments")
        group.add_argument(f"--{side}-group", required=True, help=f"{side} project group ID")
        group.add_argument(
            f"--{side}-user",
            required=(side == "source"),
            help="username (email)" + ("; defaults to --source-user" if side == "target" else ""),
        )

    parser.add_argument(
        "--data-type",
        action="append",
        choices=sorted(DATA_TYPES),
        help="copy only this data type; repeat the flag for multiple types (default: all)",
    )
    parser.add_argument(
        "--status",
        choices=["All", "Publish", "Pre-Release"],
        default="All",
        help="copy only source datasets with this status (default: All)",
    )
    parser.add_argument(
        "--service-map",
        action="append",
        default=[],
        metavar="SOURCE_SERVICE_ID=TARGET_SERVICE_ID",
        help="map source services to target services; repeatable",
    )
    parser.add_argument(
        "--default-service",
        action="append",
        default=[],
        metavar="DATA_TYPE=TARGET_SERVICE_ID",
        help="routes datasets of DATA_TYPE to a target service when no name matches; repeatable",
    )
    parser.add_argument("--dry-run", action="store_true", help="show planned copies without transferring")
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="skip waiting for target upload jobs to finish",
    )
    parser.add_argument("--job-timeout", type=float, default=1800, help="wait time in seconds for each upload job (default: 1800)")
    parser.add_argument("--poll-interval", type=float, default=10, help="seconds between job status checks (default: 10)")
    parser.add_argument("--http-timeout", type=float, default=600, help="timeout in seconds for each HTTP request (default: 600)")
    parser.add_argument("--keep-downloads", type=Path, metavar="DIR", help="save downloaded files to DIR")
    parser.add_argument("--report", type=Path, metavar="FILE", help="write the results to a JSON report in FILE")

    args = parser.parse_args(argv)
    args.source_base = resolve_base_url(args.source_env, args.source_url, "source")
    args.target_base = resolve_base_url(args.target_env, args.target_url, "target")
    args.target_user = args.target_user or args.source_user
    args.service_map = parse_pairs(args.service_map, "--service-map")
    args.default_services = parse_pairs(args.default_service, "--default-service")
    for data_type in args.default_services:
        if data_type not in DATA_TYPES:
            parser.error(f"--default-service requires one of {sorted(DATA_TYPES)}, but got {data_type!r}")
    args.data_types = args.data_type or list(DATA_TYPES)
    return args


def build_clients(args: argparse.Namespace) -> tuple[TdeiClient, TdeiClient]:
    source_password = get_password("TDEI_SOURCE_PASSWORD", f"Password for {args.source_user} ({args.source_base}): ")
    if args.target_user == args.source_user and "TDEI_TARGET_PASSWORD" not in os.environ:
        target_password = source_password
    else:
        target_password = get_password("TDEI_TARGET_PASSWORD", f"Password for {args.target_user} ({args.target_base}): ")

    source = TdeiClient(args.source_base, args.source_user, source_password, args.http_timeout)
    target = TdeiClient(args.target_base, args.target_user, target_password, args.http_timeout)
    source.authenticate()
    target.authenticate()
    return source, target


def plan(args: argparse.Namespace, source: TdeiClient, target: TdeiClient) -> list[tuple[dict, CopyResult]]:
    target_services = target.list_services(args.target_group)
    # Name and version uniquely identify datasets throughout the target
    # environment. The scope here includes only datasets within this group, so
    # we catch reruns this way while the upload request handles remaining
    # conflicts.
    existing = {
        dataset_key(d)
        for data_type in DATA_TYPES
        for d in target.list_datasets(args.target_group, data_type, "All")
        if (d.get("project_group") or {}).get("tdei_project_group_id") == args.target_group
    }
    # Running upload jobs retain their name and version until completion. The
    # dataset list omits these datasets until the corresponding job finishes.
    running = target.running_uploads(args.target_group)
    work = []
    for data_type in args.data_types:
        for dataset in source.list_datasets(args.source_group, data_type, args.status):
            # We filter by project group in the list request, but we need to
            # verify the group again to prevent datasets from other groups from
            # appearing due to API defaults.
            if (dataset.get("project_group") or {}).get("tdei_project_group_id") != args.source_group:
                continue
            name, version = dataset_key(dataset)
            result = CopyResult(
                source_id=dataset["tdei_dataset_id"],
                name=name,
                version=version,
                data_type=data_type,
                source_service=(dataset.get("service") or {}).get("name", ""),
            )
            result.target_service_id = pick_target_service(
                dataset,
                data_type,
                target_services,
                args.service_map,
                args.default_services,
            )
            if dataset_key(dataset) in existing:
                result.status = "skipped"
                result.error = "target group already has a dataset with this name and version"
            elif dataset_key(dataset) in running:
                result.status = "in-progress"
                result.job_id = running[dataset_key(dataset)]
                result.error = f"upload job {result.job_id} for this name and version is still running in the target group"
            elif not result.target_service_id:
                result.status = "skipped"
                result.error = (
                    f"no {data_type} service named {result.source_service!r} in target group; "
                    "use --service-map or --default-service"
                )
            work.append((dataset, result))
    return work


def copy_one(
    args: argparse.Namespace,
    source: TdeiClient,
    target: TdeiClient,
    dataset: dict,
    result: CopyResult,
    workdir: Path,
) -> None:
    label = f"{result.name} v{result.version}"
    stem = safe_filename(f"{result.data_type}_{result.name}_{result.version}_{result.source_id}")
    download_file = workdir / f"{stem}.download.zip"
    metadata_file = workdir / f"{stem}.metadata.json"
    metadata_file.write_text(json.dumps(dataset["metadata"], indent=2))
    files = [download_file, metadata_file]

    try:
        result.size_bytes = source.download(result.data_type, result.source_id, download_file, label)
        dataset_file, changeset_file = unwrap_download(download_file, workdir, stem)
        files += [dataset_file, changeset_file]
        result.job_id = target.upload(
            result.data_type,
            args.target_group,
            result.target_service_id,
            dataset_file,
            metadata_file,
            changeset_file,
            label,
        )
    finally:
        if not args.keep_downloads:
            for f in files:
                if f:
                    f.unlink(missing_ok=True)
    if args.no_wait:
        # We get the new dataset ID from the job once the upload is accepted.
        job = target.get_job(result.job_id, args.target_group)
        result.target_dataset_id = (job.get("response_props") or {}).get("tdei_dataset_id")
        result.status = "uploaded"
        return

    job = target.wait_for_job(result.job_id, args.target_group, args.job_timeout, args.poll_interval, label)
    props = job.get("response_props") or {}
    result.target_dataset_id = props.get("tdei_dataset_id")
    if job.get("status") == "COMPLETED":
        result.status = "copied"
    else:
        result.status = "failed"
        result.error = f"upload job {job.get('status')}: {job.get('message') or ''}".strip()


def format_size(num: int | None) -> str:
    if num is None:
        return "-"
    size = float(num)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return str(num)


def print_summary(args: argparse.Namespace, results: list[CopyResult], elapsed: float) -> None:
    print()
    print(f"Source: {args.source_base}  group {args.source_group}")
    print(f"Target: {args.target_base}  group {args.target_group}")
    print()
    if results:
        rows = [
            (
                r.status,
                r.data_type,
                f"{r.name} v{r.version}",
                r.source_id,
                r.target_dataset_id or (f"job {r.job_id}" if r.job_id else "-"),
                format_size(r.size_bytes),
            )
            for r in results
        ]
        headers = ("STATUS", "TYPE", "DATASET", "SOURCE ID", "TARGET", "SIZE")
        widths = [max(len(h), *(len(row[i]) for row in rows)) for i, h in enumerate(headers)]
        print("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
        for row in rows:
            print("  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)))
        notes = [r for r in results if r.error]
        if notes:
            print()
            print("Notes:")
            for r in notes:
                print(f"  {r.source_id} ({r.name} v{r.version}): {r.error}")
    else:
        print("No datasets found in the source project group.")

    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    total_bytes = sum(r.size_bytes or 0 for r in results)
    print()
    print(
        f"{len(results)} datasets: "
        + ", ".join(f"{n} {status}" for status, n in sorted(counts.items()))
        + f". Transferred {format_size(total_bytes)} in {elapsed:.0f}s."
    )


def write_report(path: Path, args: argparse.Namespace, results: list[CopyResult]) -> None:
    report = {
        "source": {"url": args.source_base, "project_group_id": args.source_group},
        "target": {"url": args.target_base, "project_group_id": args.target_group},
        "results": [asdict(r) for r in results],
    }
    path.write_text(json.dumps(report, indent=2))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    started = time.monotonic()

    try:
        source, target = build_clients(args)
        print(f"Listing datasets in source group {args.source_group}")
        work = plan(args, source, target)
    except (TdeiError, requests.RequestException) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    results = [r for _, r in work]
    todo = [(d, r) for d, r in work if r.status == "pending"]
    held = sum(r.status == "in-progress" for r in results)
    print(f"Found {len(work)} datasets: {len(todo)} pending, {held} in progress, {len(work) - len(todo) - held} skipped.")

    if args.dry_run:
        for _, r in todo:
            r.status = "planned"
    else:
        workdir = args.keep_downloads or Path(tempfile.mkdtemp(prefix="tdei-copy-"))
        workdir.mkdir(parents=True, exist_ok=True)
        try:
            with tqdm(total=len(todo), desc="Datasets", unit="ds") as overall:
                for dataset, result in todo:
                    overall.set_postfix_str(f"{result.name} v{result.version}")
                    t0 = time.monotonic()
                    try:
                        copy_one(args, source, target, dataset, result, workdir)
                    except (TdeiError, requests.RequestException, OSError, zipfile.BadZipFile) as exc:
                        result.status = "failed"
                        result.error = str(exc)
                    result.elapsed_s = round(time.monotonic() - t0, 1)
                    overall.write(f"{result.status:>8}  {result.data_type:<8} {result.name} v{result.version}")
                    overall.update(1)
        except KeyboardInterrupt:
            print("\nInterrupted.", file=sys.stderr)
            for r in results:
                if r.status == "pending":
                    r.status = "skipped"
                    r.error = "interrupted"
        finally:
            if not args.keep_downloads:
                shutil.rmtree(workdir, ignore_errors=True)

    print_summary(args, results, time.monotonic() - started)
    if args.report:
        write_report(args.report, args, results)
        print(f"Report written to {args.report}")
    return 1 if any(r.status == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
