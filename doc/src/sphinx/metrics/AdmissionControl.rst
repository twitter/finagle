Deadline Admission Control
<<<<<<<<<<<<<<<<<<<<<<<<<<

.. _deadline_admission_control_stats:

**admission_control/deadline/exceeded**
  A counter of the number of requests whose deadline has expired.

**admission_control/deadline/expired_ms**
  A stat of the elapsed time since expiry if a deadline has expired, in
  milliseconds.

**admission_control/deadline/transit_latency_ms**
  A stat that attempts to measure (wall time) transit times between hops, e.g.,
  from client to server. Be aware that clock drift between hosts, stop the world
  pauses, and queue backups can contribute here. Not supported by all protocols.

Nack Admission Control
<<<<<<<<<<<<<<<<<<<<<<

.. _nack_admission_control:

These metrics reflect the behavior of the
:src:`NackAdmissionFilter <com/twitter/finagle/filter/NackAdmissionFilter.scala>`.

**dropped_requests**
  A counter of the number of requests probabilistically dropped.

**accept_probability**
  A histogram of the filter's estimated probability of a request not being
  nacked.
