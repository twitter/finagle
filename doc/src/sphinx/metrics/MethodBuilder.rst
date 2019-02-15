Metrics are scoped to your client's label and method name.

The logical success and failure stats that are exported by the MethodBuilder are
distinguished from the wire stats (`clnt/<client_label>/requests`) by being
per-endpoint, but also by measuring different things.  They're both provided by
a `StatsFilter`, but at completely different layers of the finagle stack, so they
measure quite different things.  In particular, logical stats represent the
actual result you see when using the MethodBuilder client.  When you send a
request and receive a response, you will always see a single logical request.
However, the wire stats represent every time a message was sent over the wire,
so it will also include things like retries.  Logical requests also include
connection attempts, so in the opposite direction, a failed connection attempt
won't be listed under wire requests, but will be seen under logical requests.
This is very similar to what was previously captured by the "tries"-scoped
`StatsFilter` when using `ClientBuilder`.

This image shows what might happen during a single logical request, which has
three tries under the hood.  In the first try, there was a successful
service acquisition (a connection establishment) and then a request which failed.
Then there's another try, which is a failed service acquisition,
followed by the last try, where there's a successful service acquisition,
and a successful request:

.. image:: _static/logicalstats.png

- `clnt/<client_label>/<method_name>/logical/requests` — A counter of the total
  number of :ref:`logical <mb_logical_req>` successes and failures.
  This does not include any retries.
- `clnt/<client_label>/<method_name>/logical/success` — A counter of the total
  number of :ref:`logical <mb_logical_req>` successes.
- `clnt/<client_label>/<method_name>/logical/failures` — A counter of the total
  number of :ref:`logical <mb_logical_req>` failures.
- `clnt/<client_label>/<method_name>/logical/failures/<exception_name>` — A counter of the
  number of times a specific exception has caused a :ref:`logical <mb_logical_req>` failure.
- `clnt/<client_label>/<method_name>/logical/request_latency_ms` — A histogram of
  the latency of the :ref:`logical <mb_logical_req>` requests, in milliseconds.
- `clnt/<client_label>/<method_name>/retries` — A histogram of the number of times
  requests are retried.
- `clnt/<client_label>/<method_name>/backups/send_backup_after_ms` - A histogram of the time,
  in  milliseconds, after which a request will be re-issued (backup sent) if it has not yet
  completed. Present only if `idempotent` is configured.
- `clnt/<client_label>/<method_name>/backups/backups_sent` - A counter of the number of backup
  requests sent. Present only if `idempotent` is configured.
- `clnt/<client_label>/<method_name>/backups/backups_won` - A counter of the number of backup
  requests that completed before the original, regardless of whether they succeeded. Present only
  if `idempotent` is configured.
- `clnt/<client_label>/<method_name>/backups/budget_exhausted` - A counter of the number of times
  the backup request budget (computed using the current value of the `maxExtraLoad` param) or client
  retry budget was exhausted, preventing a backup from being sent. Present only if `idempotent` is
  configured.
