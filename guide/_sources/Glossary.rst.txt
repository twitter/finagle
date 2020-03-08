Glossary of Terms
=================

.. _glossary_retry_storm:

**Retry Storm**
  A retry storm is an undesirable client/server failure mode where one or more
  peers become unhealthy, causing clients to retry a significant fraction of
  requests. This has the effect of multiplying the volume of traffic sent to the
  unhealthy peers, exacerbating the problem.

  For instance, consider two services, `A` -> `B` where `B` becomes overloaded
  and starts to fail a large fraction of requests. If `A` is configured to
  greedily optimize its client success rate by retrying requests, the total
  number of requests to `B` will be substantially amplified. `B` is the victim
  of a retry storm.

.. _glossary_back_pressure:

**Back pressure**
  Back pressure is a way of signaling that a service is overloaded to its clients.
  This allows for graceful degradation of a system if one or more components are
  not performing well.

.. _glossary_nack:

**Nack**
  A nack is a Negative ACKnowledgement. A service responds with a nack to signal
  that it is refusing to process the request. Generally, this indicates that the
  request is safe to retry, but Finagle also supports non-retryable nacks.
  Responding with a nack is one way a service may exert back pressure.
