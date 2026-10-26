TDEI Project Group Copy
=======================

*tdei_copy.py* transfers all datasets from a source TDEI project group to a
target group. These groups may exist within the same environment or across
different ones (such as `dev`, `stage`, `prod`, or a custom deployment). The
script downloads each dataset from the source, uploads it along with its
metadata to the target, and outputs a summary of results.

Target groups receive these copies as Pre-Release datasets, which the script
does not publish.


Requirements
------------

The project uses [uv](https://docs.astral.sh/uv/), with dependencies declared in
`pyproject.toml`. On the first run, uv installs these and creates the project
virtualenv:

```
$ uv run tdei-copy --help
```

Resolved versions remain pinned in the `uv.lock` file. We'll need to use
`uv tool install .` or `pipx install .` if we want a standalone `tdei-copy`
command. If uv is unavailable, we need Python 3.10 or newer along with the
`requests`, `requests-toolbelt`, and `tqdm` packages, which lets us run
`python3 tdei_copy.py` instead.


Usage
-----

We can use `--dry-run` to preview a copy. This lists each dataset from the
source, selects its target service, and identifies conflicts without
transferring data:

```
$ export TDEI_SOURCE_PASSWORD='...'
$ uv run tdei-copy \
    --source-env stage --source-group 472fa55f-9cfe-47c2-9e0c-64e5fcbfa567 \
    --target-env prod --target-group f2bf8478-8dd5-4fbe-a858-b093de5a6322 \
    --source-user me@example.com --dry-run
```

We can execute the copy by removing `--dry-run` when the plan is correct. We can
use `--report copy-report.json` to output results in JSON format. If we pass
`--no-wait`, the script returns immediately after each upload reaches the
target, skipping the wait for processing to complete.

While `--help` lists all options, we'll need more detail for the following
flags.

### Environments

These names work for `--source-env` and `--target-env`:

- **`dev`:** `https://api-dev.tdei.us`
- **`stage`:** `https://api-stage.tdei.us`
- **`prod`:** `https://api.tdei.us`

We can provide a base URL via `--source-url` or `--target-url` for other
deployments.

### Credentials

The script uses username and password pairs to authenticate with both
environments. We'll need to provide these via `TDEI_SOURCE_PASSWORD` and
`TDEI_TARGET_PASSWORD`, or enter them when the script prompts us for missing
values. If we omit `--target-user`, the target uses the same account and
password as the source. The script retries requests by signing in again if a
token expires during long copies.

### Target Services

Uploads require a service within the target project group. The script defaults
to a target service that matches the source service's name and data type. When
these names differ, we can map them with these flags:

- **`--service-map SOURCE_SERVICE_ID=TARGET_SERVICE_ID`:** routes datasets from
  a specific source service to a specific target service.
- **`--default-service DATA_TYPE=TARGET_SERVICE_ID`:** handles datasets of a
  certain data type (`osw`, `flex`, or `pathways`) when no other match exists.

We can repeat both flags. The script skips any dataset without a matching
service and includes it in the summary.


Duplicate Names and Reruns
--------------------------

TDEI requires that every dataset name and version pair remains unique within an
environment, so the target returns HTTP 400 when an upload repeats one. This
uniqueness extends to active upload jobs, which reserve the name and version
until they finish, even though the dataset doesn't show in the list during that
time.

We can safely rerun the script after partial failures because it checks the
target group for both existing datasets and active jobs before uploading. The
script skips datasets that already arrived and ignores those with jobs still
running. Since Pre-Release datasets in different project groups stay hidden from
the script, names used there still trigger 400 upload errors from the API.


Copy Process
------------

The download API provides a wrapper zip containing a dataset zip, a
*metadata.json*, and occasionally a changeset file (*.osc* or *.zip*). The
script unwraps this download first because the upload API requires those items
as distinct form fields. When the wrapper holds individual data files rather
than a dataset zip, the script bundles those files into a new zip.

The script pulls the metadata for each upload from the source dataset listing.
Downloads occupy a temporary directory only until their upload finishes, unless
we use `--keep-downloads DIR` to preserve them.


Results
-------

The summary table assigns one of these statuses to each dataset:

- **`copied`:** the target completed processing the upload.
- **`uploaded`:** the target accepted the upload and we used `--no-wait` to skip
  the processing wait.
- **`in-progress`:** the target group has an active upload job for this version
  and name, so the script skips another upload.
- **`skipped`:** no target service matched, or the target group already contains
  this version and name.
- **`failed`:** a job for processing, uploading, or downloading failed (see
  notes below the table for reasons).
- **`planned`:** the dataset would be copied if we used `--dry-run`.

The TARGET column contains the job ID unless the upload job reports a new
dataset ID, in which case it shows that ID. The script provides the new dataset
ID even when we use `--no-wait` because the job records it upon target
acceptance.

Exit codes depend on the outcome: 0 if no datasets failed, 1 if at least one
failed, and 2 if the script failed to list datasets or sign in.

If we use `--no-wait`, processing failures that happen later won't show in the
summary. We'll see them if we rerun the command, because a failed upload creates
no dataset and the rerun uploads it again.
