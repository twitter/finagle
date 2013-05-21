Finagle!
========

.. image:: _static/governor.png
   :class: floatingflask

Finagle is an extensible RPC system for the JVM, used to construct
high-concurrency servers. Finagle implements uniform client and
server APIs for several protocols, and is designed for high
performance and concurrency. Most of Finagle's code is protocol
agnostic, simplifying the implementation of new protocols.

Finagle uses a *clean*, *simple*, and *safe* concurrent programming
model, based on :doc:`Futures <Futures>`. This leads to safe and
modular programs that are also simple to reason about.

Finagle clients and servers expose statistics for monitoring and
diagnostics. They are also traceable through a mechanism similar to
Dapper_'s (another Twitter open source project, Zipkin_, provides
trace aggregation and visualization).

The :doc:`quickstart <Quickstart>` has an overview of the most
important concepts, walking you through the setup of a simple HTTP
server and client.

A section on :doc:`Futures <Futures>` follows, motivating and
explaining the important ideas behind the concurrent programming 
model used in Finagle. The next section documents 
:doc:`Services & Filters <ServicesAndFilters>` which are the core 
abstractions used to represent clients and servers and modify 
their behavior.

Other useful resources include:

- `Twitter engineering blog entry introducing Finagle <http://engineering.twitter.com/2011/08/finagle-protocol-agnostic-rpc-system.html>`_
- `ScalaDays 2011 presentation on Finagle <http://days2011.scala-lang.org/node/138/286>`_
- Twitter's `Scala School <http://twitter.github.com/scala_school/>`_ has a section `introducing Finagle <http://twitter.github.com/scala_school/finagle.html>`_ and another `constructing a distributed search engine using Finagle <http://twitter.github.com/scala_school/searchbird.html>`_

.. _Dapper: http://research.google.com/pubs/pub36356.html
.. _Zipkin: http://twitter.github.com/zipkin/

User's guide
------------

.. toctree::
   :maxdepth: 4
   
   Quickstart
   Futures
   ServicesAndFilters
   Metrics
   FAQ

Notes
-----

.. toctree::
   :maxdepth: 2

   changelog
   license

.. API reference,
  additional, etc.  as different ones
  examples
  protocol specific docs?
  scaladoc
