Deadline Admission Control
<<<<<<<<<<<<<<<<<<<<<<<<<<

.. _deadline_admission_control_stats:

**admission_control/deadline/exceeded**
  A counter of the number of requests whose deadline has expired.

**admission_control/deadline/expired_ms**
  A stat of the elapsed time since expiry if a deadline has expired, in
  milliseconds.


Nack Admission Control
<<<<<<<<<<<<<<<<<<<<<<

.. _nack_admission_control:

These metrics reflect the behavior of the
:src:`NackAdmissionFilter <com/twitter/finagle/filter/NackAdmissionFilter.scala>`.

**dropped_requests**
  A counter of the number of requests probabilistically dropped.
