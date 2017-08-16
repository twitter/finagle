FailureAccrualFactory
<<<<<<<<<<<<<<<<<<<<<

**removed_for_ms**
  A counter of the total time in milliseconds any host has spent in dead
  state due to failure accrual.

**probes**
  A counter of the number of requests sent through failure accrual while
  a host was marked dead to probe for revival.

**removals**
  A count of how many times any host has been removed due to failure
  accrual.  Note that there is no specificity on which host in the
  cluster has been removed, so a high value here could be one
  problem-child or aggregate problems across all hosts.

**revivals**
  A count of how many times a previously-removed host has been
  reactivated after the penalty period has elapsed.
