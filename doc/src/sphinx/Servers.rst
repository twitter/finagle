Servers
=======

Finagle servers implement a simple :src:`interface: <com/twitter/finagle/Server.scala>`

.. code-block:: scala

  def serve(
    addr: SocketAddress,
    factory: ServiceFactory[Req, Rep]
  ): ListeningServer

When given a `SocketAddress` and a :ref:`ServiceFactory <service_factory>`, a server returns a handle
to a `ListeningServer`. The handle allows for management of server resources. The interface comes with
variants that allow for serving a simple `Service` as well. Typical usage takes the form
``Protocol.serve(...)``, for example:

.. code-block:: scala

  val server = Http.serve(":8080", myService)
  Await.ready(server) // waits until the server resources are released

However, `finagle-thrift` servers expose a rich API because their interfaces are defined
via a thrift IDL. See the protocols section on :ref:`Thrift <thrift_and_scrooge>`
for more details.

Server Modules
--------------

Finagle servers are simple; they are designed to serve requests quickly. As such,
Finagle minimally furnishes servers with additional behavior. More sophisticated
behavior lives in the :ref:`clients <finagle_clients>`. However, the server does come
with some useful modules to help owners observe and debug their servers. This includes
:src:`Monitoring <com/twitter/finagle/filter/MonitorFilter.scala>`,
:src:`Tracing <com/twitter/finagle/tracing/TraceInitializerFilter.scala>`,
and :src:`Stats <com/twitter/finagle/service/StatsFilter.scala>`.
Additionally, Finagle servers :ref:`propagate failure <propagate_failure>`.

Configuration
-------------
Prior to :doc:`6.x <changelog>`, the `ServerBuilder` was the primary method for configuring
the modules inside a Finagle server. We've moved away from this model for various
:ref:`reasons <configuring_finagle6>`.
