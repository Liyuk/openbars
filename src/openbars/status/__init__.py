"""Run completion artefacts: the ``_status`` files and optional webhook delivery.

* :mod:`openbars.status.run_status` -- schema-v1 run status documents, written atomically
  under ``<storage.path>/_status/`` at the end of every ``run``.
* :mod:`openbars.status.webhook` -- signed, retried delivery of that document to the
  ``notify.webhooks`` targets.

OpenBars only writes its own data directory here and knows nothing about the systems that
consume these files.
"""

from openbars.status.run_status import (  # noqa: F401
    SCHEMA,
    SCHEMA_VERSION,
    RunStatusRecorder,
    latest_status_path,
    new_run_id,
    read_latest_status,
    render_document,
    write_run_status,
)
