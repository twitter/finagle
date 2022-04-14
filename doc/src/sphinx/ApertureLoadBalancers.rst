.. _aperture_load_balancers:

Aperture Load Balancers
=======================

The family of aperture load balancers are characterized by their selection of a subset of backend
hosts with which to direct traffic to. This stands in contrast to :ref:`P2C <p2c_least_loaded>`
which establishes connections to all available backends. In Finagle, there are two aperture load
balancers, random aperture and deterministic aperture which differ in how they select the subset of
hosts. Both of these balancers can behave as weighted apertures, which have knowledge of endpoint
weights and route traffic accordingly without requiring preprocessing.

Eager Connections
-----------------
Aperture Load Balancers also have a special feature called "eager connections". What this means is
that they open connections to the remote peers in their aperture eagerly. This can save time when a
service is starting up, so that its persistent connections can get going quickly and the initial
requests don't need to wait for TCP connection establishment. Eager connections can be enabled or
disabled service-wide with the flag
`com.twitter.finagle.loadbalancer.exp.apertureEagerConnections`. That flag can be set to one of
three values, `Enabled`, `Disabled`, and `ForceWithDefault` (case doesn't matter).

- `Enabled` turns on eager connections for the cluster that the client is configured to talk to.
- `Disabled` disables eager connections always.
- `ForceWithDefault` turns on eager connections for the cluster that the client is configured to
  talk to, as well as other clusters that the client finds itself routing to for name rewrites via
  dtabs. This can be useful when a service sees a small number of well-known dtabs, but can be risky
  if a service expects many dtabs and can risk creating too many connections.

.. include:: loadbalancers/RandomAperture.rst

.. include:: loadbalancers/DeterministicAperture.rst

.. include:: loadbalancers/WeightedAperture.rst
