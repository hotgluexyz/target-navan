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
_OPTIONS_LIST_PAGE_SIZE = 100
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
            existing = self._list_existing_options(field_name)
            for start in range(0, len(field_records), _OPTIONS_CHUNK_SIZE):
                chunk = field_records[start : start + _OPTIONS_CHUNK_SIZE]
                self._submit_options_chunk(field_name, chunk, existing)

    def _list_existing_options(self, field_name: str) -> dict[str, str]:
        """Return ``{value: display_value}`` for options already on the field."""
        endpoint = f"/expense/custom-fields/{field_name}/options"
        existing: dict[str, str] = {}
        page = 0

        while True:
            response = self.request_api(
                "GET",
                endpoint=endpoint,
                params={"page": page, "page_size": _OPTIONS_LIST_PAGE_SIZE},
            )
            body = response.json()
            for option in body.get("options") or []:
                value = option.get("value")
                if value:
                    existing[str(value)] = option.get("display_value") or str(value)

            total_pages = int(body.get("total_pages") or 1)
            page += 1
            if page >= total_pages:
                break

        self.logger.info(
            "Found %s existing option(s) for Navan field '%s'",
            len(existing),
            field_name,
        )
        return existing

    def _submit_options_chunk(
        self,
        field_name: str,
        records: list[dict],
        existing: dict[str, str],
    ) -> None:
        """Skip unchanged options; POST new/updated ones and write per-record state."""
        to_skip: list[dict] = []
        to_submit: list[dict] = []
        for record in records:
            prior_display = existing.get(record["value"])
            if (
                prior_display is not None
                and prior_display == record["display_value"]
            ):
                to_skip.append(record)
            else:
                to_submit.append(record)

        for record in to_skip:
            self._record_option_state(
                field_name, record, is_duplicate=True
            )

        if not to_submit:
            self.logger.info(
                "Skipping Navan options POST for field '%s': "
                "all %s option(s) already exist unchanged",
                field_name,
                len(to_skip),
            )
            return

        self.logger.info(
            "Submitting %s option(s) for field '%s' "
            "(%s unchanged skipped)",
            len(to_submit),
            field_name,
            len(to_skip),
        )
        response = self.make_batch_request(to_submit)

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
        self._record_submitted_options_state(
            field_name, to_submit, existing, job
        )

    def _record_submitted_options_state(
        self,
        field_name: str,
        records: list[dict],
        existing: dict[str, str],
        job: dict,
    ) -> None:
        """Write per-record state from a finished Navan options job."""
        status = job.get("status")
        failed_by_index = {
            error["operation_index"]: error
            for error in (job.get("errors") or [])
            if "operation_index" in error
        }

        for index, record in enumerate(records):
            error = failed_by_index.get(index)
            if error is not None:
                self._record_option_state(
                    field_name,
                    record,
                    success=False,
                    error=error.get("error_message"),
                )
                continue

            if status == "FAILED":
                self._record_option_state(
                    field_name,
                    record,
                    success=False,
                    error=(
                        f"Navan custom-field options job FAILED for "
                        f"'{field_name}'"
                    ),
                )
                continue

            if record["value"] in existing:
                self._record_option_state(
                    field_name, record, is_updated=True
                )
            else:
                self._record_option_state(field_name, record)
            existing[record["value"]] = record["display_value"]

    def _record_option_state(
        self,
        field_name: str,
        record: dict,
        *,
        success: bool = True,
        is_duplicate: bool = False,
        is_updated: bool = False,
        error: str | None = None,
    ) -> None:
        """Write one export-state row for a custom-field option."""
        state = {
            "hash": self.build_record_hash(record),
            "success": success,
            "id": f"{field_name}:{record['value']}",
            "externalId": record["value"],
        }
        if is_updated:
            state["is_updated"] = True
        if error is not None:
            state["error"] = error
        self.update_state(
            state, is_duplicate=is_duplicate, record=record
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
                self.logger.info(
                    "Navan custom-field job %s finished: total=%s succeeded=%s "
                    "failed=%s",
                    job_id,
                    job.get("total_operations"),
                    job.get("succeeded_operations"),
                    job.get("failed_operations"),
                )
                return job
            time.sleep(delay)
            delay = min(delay * 2, _OPTIONS_POLL_MAX_SECONDS)

        raise RuntimeError(
            f"Timed out polling Navan custom-field job {job_id} after "
            f"{_OPTIONS_POLL_MAX_ATTEMPTS} attempts"
        )
