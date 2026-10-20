"""Navan write sinks.

After a Navan → Rillet job finishes, the Rillet webhook handler POSTs sync
status records into Hotglue as a real-time write on ``transactions_sync``.
This sink PATCHes Navan so those transactions leave ``READY_TO_EXPORT`` and
deep-link back to the Rillet charge or reimbursement.
"""

from __future__ import annotations

from target_navan.client import NavanSink

# Fields the webhook handler sends; anything else is dropped before PATCH.
_SYNC_FIELDS = (
    "id",
    "erp_sync_status",
    "external_txn_id",
    "external_txn_link",
    "external_txn_type",
    "external_erp_name",
)


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
