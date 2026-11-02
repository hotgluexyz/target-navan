"""Navan write sinks.

``transactions_sync`` — after a Navan → Rillet job finishes, the Rillet webhook
handler POSTs sync status records into Hotglue as a real-time write. This sink
PATCHes Navan so those transactions leave ``READY_TO_EXPORT`` and deep-link back
to the Rillet charge or reimbursement.

``custom_field_options`` — one-time / infrequent Rillet → Navan seeding of
department and subsidiary option values via Navan's async custom-fields options
API (HGI-11574).
"""

from __future__ import annotations

import time
from collections import defaultdict

from target_navan.client import NavanBatchSink, NavanSink

# Fields the webhook handler sends; anything else is dropped before PATCH.
_SYNC_FIELDS = (
    "id",
    "erp_sync_status",
    "external_txn_id",
    "external_txn_link",
    "external_txn_type",
    "external_erp_name",
)

_OPTIONS_TERMINAL_STATUSES = frozenset(
    {"COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED"}
)
_OPTIONS_CHUNK_SIZE = 5000
_OPTIONS_POLL_INITIAL_SECONDS = 1.0
_OPTIONS_POLL_MAX_SECONDS = 30.0
_OPTIONS_POLL_MAX_ATTEMPTS = 60


class TransactionsSyncSink(NavanSink):
    """Mark Navan expense transactions as synced to Rillet.

    Navan's ``PATCH /expense/transactions`` accepts up to 100 rows per call.
    We send one transaction per request so a single failure is easy to attribute
    in Hotglue export details (same pattern as Airwallex ``bills_sync``).
    """

    name = "transactions_sync"
    endpoint = "/expense/transactions"

    def upsert_record(self, record: dict, context: dict):
        """PATCH one Navan transaction with ERP sync metadata.

        Expected record shape (from the webhook handler)::

            {
              "id": "<navan-transaction-uuid>",
              "erp_sync_status": "SYNCED",
              "external_txn_id": "<rillet-id>",
              "external_txn_link": "https://sandbox.rillet.io/...",
              "external_txn_type": "charge" | "reimbursement",
              "external_erp_name": "Rillet"
            }

        Returns:
            Tuple of (navan_transaction_id, success, state_updates).
        """
        transaction_id = record.get("id")
        if not transaction_id:
            raise ValueError(
                "Navan transaction id is required to update erp_sync_status"
            )

        payload = {key: record[key] for key in _SYNC_FIELDS if key in record}
        response = self.request_api(
            "PATCH",
            endpoint=self.endpoint,
            request_data={"transactions": [payload]},
        )
        return transaction_id, response.ok, {"is_updated": True}


class CustomFieldOptionsSink(NavanBatchSink):
    """UPSERT Navan custom-field options from Rillet fields / subsidiaries.

    Records are accumulated by ``field_name``, then submitted via::

        POST /expense/custom-fields/{fieldName}/options

    Each async job is polled until terminal status.
    """

    name = "custom_field_options"
    endpoint = "/expense/custom-fields"
    max_size = _OPTIONS_CHUNK_SIZE

    def process_batch_record(self, record: dict, index: int) -> dict:
        """Normalize one Singer record into a Navan options operation."""
        field_name = record.get("field_name")
        value = record.get("value")
        if not field_name:
            raise ValueError(
                "custom_field_options records require field_name "
                "(Navan custom field API name)"
            )
        if not value:
            raise ValueError(
                "custom_field_options records require value "
                "(stable option identifier)"
            )

        display_value = record.get("display_value") or value
        action = record.get("action") or "UPSERT"
        return {
            "field_name": field_name,
            "action": action,
            "value": value,
            "display_value": display_value,
        }

    def make_batch_request(self, records: list[dict]):
        """POST option UPSERTs for one field (all records share ``field_name``)."""
        field_name = records[0]["field_name"]
        operations = [
            {
                "action": record["action"],
                "value": record["value"],
                "display_value": record["display_value"],
            }
            for record in records
        ]
        endpoint = f"/expense/custom-fields/{field_name}/options"
        self.logger.info(
            "Submitting %s custom-field option operations for field '%s'",
            len(operations),
            field_name,
        )
        return self.request_api(
            "POST",
            endpoint=endpoint,
            request_data={"operations": operations},
        )

    def process_batch(self, context: dict) -> None:
        """Group records by field_name, POST each chunk, and record export state."""
        if not self.latest_state:
            self.init_state()

        raw_records = context.get("records") or []
        if not raw_records:
            return

        records = [
            self.process_batch_record(record, index)
            for index, record in enumerate(raw_records)
        ]

        by_field: dict[str, list[dict]] = defaultdict(list)
        for record in records:
            by_field[record["field_name"]].append(record)

        for field_name, field_records in by_field.items():
            for start in range(0, len(field_records), _OPTIONS_CHUNK_SIZE):
                chunk = field_records[start : start + _OPTIONS_CHUNK_SIZE]
                self._submit_options_chunk(field_name, chunk)

    def _submit_options_chunk(self, field_name: str, records: list[dict]) -> None:
        """Submit one options chunk, poll the job, and write per-record state."""
        response = self.make_batch_request(records)

        if response.status_code not in (200, 202):
            raise RuntimeError(
                f"Navan options POST for '{field_name}' returned "
                f"{response.status_code}: {response.text}"
            )

        job_id = self._extract_job_id(response)
        if not job_id:
            raise RuntimeError(
                f"Navan options POST for '{field_name}' returned no job_id "
                f"(status={response.status_code}, body={response.text})"
            )

        job = self._poll_options_job(job_id)
        self._raise_for_job_errors(field_name, job)

        for record in records:
            self.update_state(
                {
                    "hash": self.build_record_hash(record),
                    "success": True,
                    "id": f"{field_name}:{record['value']}",
                    "job_id": job_id,
                    "field_name": field_name,
                    "value": record["value"],
                    "display_value": record["display_value"],
                },
                record=record,
            )

    def _extract_job_id(self, response) -> str | None:
        """Read ``job_id`` from the POST body or ``Location`` header."""
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {}

        job_id = body.get("job_id")
        if job_id:
            return str(job_id)

        location = response.headers.get("Location") or response.headers.get("location")
        if not location:
            return None
        return location.rstrip("/").split("/")[-1] or None

    def _poll_options_job(self, job_id: str) -> dict:
        """Poll the async options job until it reaches a terminal status."""
        endpoint = f"/expense/custom-fields/jobs/{job_id}"
        delay = _OPTIONS_POLL_INITIAL_SECONDS

        for attempt in range(1, _OPTIONS_POLL_MAX_ATTEMPTS + 1):
            response = self.request_api("GET", endpoint=endpoint)
            job = response.json()
            status = job.get("status")
            self.logger.info(
                "Navan custom-field job %s status=%s (attempt %s/%s)",
                job_id,
                status,
                attempt,
                _OPTIONS_POLL_MAX_ATTEMPTS,
            )
            if status in _OPTIONS_TERMINAL_STATUSES:
                return job
            time.sleep(delay)
            delay = min(delay * 2, _OPTIONS_POLL_MAX_SECONDS)

        raise RuntimeError(
            f"Timed out polling Navan custom-field job {job_id} after "
            f"{_OPTIONS_POLL_MAX_ATTEMPTS} attempts"
        )

    def _raise_for_job_errors(self, field_name: str, job: dict) -> None:
        """Raise when the Navan job failed or reported failed operations."""
        status = job.get("status")
        failed = int(job.get("failed_operations") or 0)
        errors = job.get("errors") or []

        if status == "FAILED" or failed > 0 or status == "COMPLETED_WITH_ERRORS":
            raise RuntimeError(
                f"Navan custom-field options job failed for '{field_name}': "
                f"status={status}, failed_operations={failed}, errors={errors}"
            )
