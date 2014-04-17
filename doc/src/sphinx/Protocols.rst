Protocols
=========

The core of Finagle is protocol-agnostic, meaning its internals provide an
extensible RPC subsystem without defining any details of specific
client-server protocols. Thus in order to provide usable APIs for clients and
servers, there are a number of Finagle subprojects that implement common
protocols. A few of these protocol implementations are documented below.

.. _thrift_and_scrooge:

Thrift
------

`Apache Thrift <http://thrift.apache.org/>`_ is an interface definition
language. With its associated code generator(s) and binary communication
protocol, Thrift facilitates the development of scalable RPC systems. By
“scalable”, we specifically mean that IDLs can be shared, allowing developers
to define schemas and expose network services that clients access using code
generated for their preferred programming language.

The IDL is the core of Thrift. IDLs provide clear service specifications that we
can used to implement clients and servers. This means that different
implementations of servers can be swapped out transparently, since they all
expose the same interface, regardless of language.

Thrift was originally built at Facebook. For more details on the original design,
check out the `whitepaper <http://thrift.apache.org/static/files/thrift-20070401.pdf>`_.

`finagle-thrift` is used extensively within Twitter, but to meet the needs of our
service-oriented architecture we had to extend the Thrift protocol. We
introduced the notion of “Twitter-upgraded Thrift”, or TTwitter, which augments
the protocol with support for our internal infrastructure. Specifically, we tack
on a request header containing Zipkin tracing info, Finagle ClientId strings, and
Wily delegations. In order to maintain backwards compatibility, TTwitter clients
perform protocol negotiation upon connection and will downgrade to raw TBinary
Thrift if servers are not using the upgraded protocol. By default, `finagle-thrift`
uses the Thrift framed codec and the binary protocol for serialization.

Using finagle-thrift
~~~~~~~~~~~~~~~~~~~~

At Twitter, we use our open-source Thrift code-generator called
`Scrooge <http://twitter.github.io/scrooge/>`_. Scrooge is written in Scala and
can generate source code in Scala or Java. Given the following IDL:

.. literalinclude:: ../../../finagle-example/src/main/thrift/hello.thrift
   :lines: 4-7

Scrooge will generate code that can be used by `finagle-thrift` with the
following rich APIs [#]_:

.. _finagle_thrift_server:

Serving the IDL:

.. includecode:: ../../../finagle-example/src/main/scala/com/twitter/finagle/example/thrift/ThriftServer.scala#thriftserverapi

.. _finagle_thrift_client:

and the symmetric remote dispatch:

.. includecode:: ../../../finagle-example/src/main/scala/com/twitter/finagle/example/thrift/ThriftClient.scala#thriftclientapi

Check out the `finagle-thrift` `API docs <http://twitter.github.io/finagle/docs/#com.twitter.finagle.Thrift$>`_
for more info.

.. [#] This API makes it difficult to wrap endpoints in Finagle
       filters. We're still experimenting with how to make the `Service`
       abstraction fit more cleanly into a world with IDLs.

Mux
---

**What is Mux?**

At its core, Mux is a generic RPC multiplexing protocol. Although its primary
implementation is as a Finagle﻿ subproject, Mux is not Finagle-specific. In the
same way that HTTP is an application-layer protocol with ﻿﻿﻿numerous implementations
in a variety of languages, Mux is a session-layer protocol with a Scala
implementation in the finagle-mux package. Also since it is a purely
session-layer protocol, Mux can be used in conjunction with protocols from other
layers of the `OSI <http://en.wikipedia.org/wiki/OSI_model>`_ model. For example,
Finagle currently has an implementation of the Thrift protocol on top of Mux,
available in the `finagle-thriftmux` package.

Much of the future work on Finagle will involve improvements to Mux and feature
development targeting services that support it. The wire format and semantics of
the Mux protocol are documented in `its source
code <https://github.com/twitter/finagle/blob/master/finagle-mux/src/main/scala/com/twitter/finagle/mux/package.scala>`_.

**Why is RPC multiplexing important?**

Some important consequences of multiplexing at the RPC level:

- One network connection per client-server session
- Maximization of available bandwidth without incurring the cost of opening
  additional sockets
- Elimination of head-of-line blocking
- Explicit queue management

**Mux as a pure session-layer protocol**

In OSI terminology, Mux is a pure
session layer protocol. As such, it provides a rich set of session control
features:

*Session Liveness Detection*

In the past, Finagle has relied on mechanisms like failure accrual to detect the
health of an endpoint. These mechanisms require configuration which tended to
vary across disparate services and endpoints. Mux introduces a principled way to
gauge session health via ping messages. This allows Finagle to provide more
generic facilities for liveness detection that require little-to-no
configuration.

*Request Cancellation*

Without proper protocol support, request cancellation in Finagle has historically
been difficult to implement efficiently. For example, something as simple as
timing out a request requires closing its TCP connection. Mux gives us granular
control over request cancellation without having to go so far as to terminate a
session.

*Ability to advertise session windows*

Mux enables servers to advertise availability on a per-window basis. This is
useful for establishing explicit queueing policies, leading the way to
intelligent back-pressure, slow start, and GC-avoidance.
