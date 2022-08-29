Deadline Admission Control
<<<<<<<<<<<<<<<<<<<<<<<<<<

.. _deadline_admission_control_stats:

Note that if the deadline filter is in darkmode (which is the default),
these metrics will have the prefix `admission_control/darkmode_deadline`
instead of `admission_control/deadline`. 

**admission_control/deadline/exceeded**
  A counter of the number of requests whose deadline has expired.

**admission_control/deadline/expired_ms**
  A stat of the elapsed time since expiry if a deadline has expired, in
  milliseconds.

**admission_control/deadline/remaining_ms**
  A stat of the remaining time in a deadline if it has not expired, in
  milliseconds.


Nack Admission Control
<<<<<<<<<<<<<<<<<<<<<<

.. _nack_admission_control:

These metrics reflect the behavior of the
:src:`NackAdmissionFilter <com/twitter/finagle/filter/NackAdmissionFilter.scala>`.

**dropped_requests**
  A counter of the number of requests probabilistically dropped.

**nonretryable**
  A counter of the number of requests that were deemed non-retryable and thus
  were not passed through the set of nack admission filters.

**ema_value**
  A gauge of the EMA value. Between 0 and 100, inclusive.
