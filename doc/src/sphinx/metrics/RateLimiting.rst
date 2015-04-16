RateLimitingFilter
<<<<<<<<<<<<<<<<<<

**refused**
  a counter of the number of refused connections by the rate limiting filter

RetryingFilter
<<<<<<<<<<<<<<

There is a "retries" stat that's scoped to "automatic", which reenqueues
automatically when finagle detects that it's safe to reenqueue a request,
namely when finagle sees that it was never written to the wire.  These
reenqueues are handled by finagle, and so don't sap your retry budget.

**retries**
  a histogram of the number of times the service had to retry
