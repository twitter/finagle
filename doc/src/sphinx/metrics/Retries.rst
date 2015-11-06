These metrics track the retries of failed requests
via the `com.twitter.finagle.service.Retries` module.

Requeues represent requests that were automatically retried by Finagle.
Only failures which are known to be safe are eligible to be requeued.
The number of retries allowed are controlled by a dynamic budget, `RetryBudget`.

For clients built using `ClientBuilder`, the **retries** stat represents retries
handled by the configured `RetryPolicy`. Note that application level failures
**are not** included, which is particularly important for protocols that include
exceptions, such as Thrift. The number of retries allowed is controlled by the
same dynamic budget used for requeues.

Somewhat confusingly for clients created via `ClientBuilder` there
are an additional set of metrics scoped to `tries` that come from `StatsFilter`.
Those metrics represent logical requests, while the metrics below
are for the  physical requests, including the retries. You can replicate
this behavior for clients built with the `Stack` API by wrapping the service
with a `StatsFilter` scoped to `tries`.

**retries**
  a stat of the number of times requests are retried as per a policy
  defined by the `RetryPolicy` from a `ClientBuilder`.

**retries/requeues**
  a counter of the number of times requests are requeued. Failed requests which are
  eligible for requeues are failures which are known to be safe — see
  `com.twitter.finagle.service.RetryPolicy.RetryableWriteException`.

**retries/budget**
  a gauge of the currently available retry budget

**retries/budget_exhausted**
  a counter of the number of times when the budget is exhausted
