These metrics track the retries of failed requests
via the
:src:`Retries module <com/twitter/finagle/service/Retries.scala>`.


Requeues represent requests that were automatically retried by Finagle.
Only failures which are known to be safe are eligible to be requeued.
The number of retries allowed are controlled by a dynamic budget,
:src:`RetryBudget <com/twitter/finagle/service/RetryBudget.scala>`.

For clients built using
:src:`ClientBuilder <com/twitter/finagle/builder/ClientBuilder.scala>`,
the `retries` stat represents retries
handled by the configured
:src:`RetryPolicy <com/twitter/finagle/service/RetryPolicy.scala>`.
Note that application level failures
**are not** included, which is particularly important for protocols that include
exceptions, such as Thrift. The number of retries allowed is controlled by the
same dynamic budget used for requeues.

Somewhat confusingly for clients created via ``ClientBuilder`` there
are an additional set of metrics scoped to `tries` that come from ``StatsFilter``.
Those metrics represent logical requests, while the metrics below
are for the  physical requests, including the retries. You can replicate
this behavior for clients built with the ``Stack`` API by wrapping the service
with a ``StatsFilter`` scoped to `tries`.

**retries**
  A stat of the number of times requests are retried as per a policy
  defined by the ``RetryPolicy`` from a ``ClientBuilder``.

**retries/requeues**
  A counter of the number of times requests are requeued. Failed requests which are
  eligible for requeues are failures which are known to be safe — see
  ``com.twitter.finagle.service.RetryPolicy.RetryableWriteException``.

**retries/requeues_per_request**
  A stat of the number of times requests are requeued.

**retries/budget**
  A gauge of the currently available retry budget.

**retries/budget_exhausted**
  A counter of the number of times when the budget is exhausted.

**retries/request_limit**
  A counter of the number of times the limit of retry attempts for a logical
  request has been reached.

**retries/not_open**
  A counter of the number of times a request was deemed retryable but
  was not retried due to the underlying ``Service`` not having a status
  of ``Open``.

**retries/cannot_retry**
  A counter of the number of times a request was deemed requeueable but
  was not requeued due to the underlying ``ServiceFactory`` not having a
  status of ``Open``.
