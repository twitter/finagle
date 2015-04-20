RateLimitingFilter
<<<<<<<<<<<<<<<<<<

**refused**
  a counter of the number of refused connections by the rate limiting filter

Requeue
<<<<<<<

There is a "requeues" stat that's scoped to "requeue", which tracks
requests that fail (or failures to establish a session) but have been
automatically re-enqueued when finagle detects that it's safe.
These re-enqueues are credited by finagle and they don't sap your
retry budget.

**requeues**
  a counter of the number of times the service had to retry

**budget**
  a gauge representing the instantaneous requeue budget
