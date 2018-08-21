Metrics are scoped to your client's label and method name.

- `clnt/your_client_label/method_name/logical/requests` — A counter of the total
  number of :ref:`logical <mb_logical_req>` successes and failures.
  This does not include any retries.
- `clnt/your_client_label/method_name/logical/success` — A counter of the total
  number of :ref:`logical <mb_logical_req>` successes.
- `clnt/your_client_label/method_name/logical/failures/exception_name` — A counter of the
  number of times a specific exception has caused a :ref:`logical <mb_logical_req>` failure.
- `clnt/your_client_label/method_name/logical/request_latency_ms` — A stat of
  the latency of the :ref:`logical <mb_logical_req>` requests, in milliseconds.
- `clnt/your_client_label/method_name/retries` — A stat of the number of times
  requests are retried.
- `clnt/your_client_label/method_name/backups/send_backup_after_ms` - A stat of the time,
  in  milliseconds, after which a request will be re-issued (backup sent) if it has not yet
  completed. Present only if `idempotent` is configured.
- `clnt/your_client_label/method_name/backups/backups_sent` - A counter of the number of backup
  requests sent. Present only if `idempotent` is configured.
- `clnt/your_client_label/method_name/backups/backups_won` - A counter of the number of backup
  requests that completed before the original, regardless of whether they succeeded. Present only
  if `idempotent` is configured.
- `clnt/your_client_label/method_name/backups/budget_exhausted` - A counter of the number of times
  the backup request budget (computed using the current value of the `maxExtraLoad` param) or client
  retry budget was exhausted, preventing a backup from being sent. Present only if `idempotent` is
  configured.
