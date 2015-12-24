Deadline Admission Control
<<<<<<<<<<<<<<<<<<<<<<<<<<

.. _deadline_admission_control_stats:

**admission_control/deadline/exceeded**
  A counter of the number of requests whose deadline has expired, where the
  elapsed time since expiry is within the configured tolerance.

**admission_control/deadline/exceeded_beyond_tolerance**
  A counter of the number of requests whose deadline has expired, where the
  elapsed time since expiry is beyond the configured tolerance.

**admission_control/deadline/rejected**
  A counter of the number of requests rejected for being past their deadline.
