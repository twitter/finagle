Quickstart
==========

In this section we'll use Finagle to build a very simple HTTP server
that is also an HTTP client — an HTTP proxy. We assume that you
are familiar with Scala_ (if not, may we recommend
`Scala School <https://twitter.github.io/scala_school/>`_).

.. _Scala: https://www.scala-lang.org

The entire example is available, together with a self-contained
script to launch sbt, in the Finagle git repository:

::

	$ git clone https://github.com/twitter/finagle.git
	$ cd finagle/doc/src/sphinx/code/quickstart
	$ ./sbt compile

Setting up SBT
--------------

We'll use sbt_ to build our project. Finagle is published to Maven Central,
so little setup is needed: see the ``build.sbt`` in the ``quickstart`` directory.
We no longer support ``scala`` ``2.10``.

.. parsed-literal::

	name := "quickstart"

	version := "1.0"

	libraryDependencies += "com.twitter" %% "finagle-http" % "|release|"

Any file in this directory will now be compiled by ``sbt``.

.. _sbt: https://www.scala-sbt.org

A minimal HTTP server
---------------------

We'll need to import a few things into our namespace.

.. includecode:: code/quickstart/Server.scala#imports
   :language: scala

``Service`` is the interface used to represent a server or a client
(:doc:`about which more later <ServicesAndFilters>`). ``Http`` is Finagle's HTTP
client and server.

Next, we'll define a ``Service`` to serve our HTTP requests:

.. includecode:: code/quickstart/Server.scala#service
   :language: scala

Services are functions from a request type (``com.twitter.finagle.http.Request``)
to a ``Future`` of a response type (``com.twitter.finagle.http.Response``).
Put another way: given a *request*, we must promise a *response* some
time in the future. In this case, we just return a trivial HTTP-200
response immediately (through ``Future.value``), using the same
version of HTTP with which the request was dispatched.

Now that we've defined our ``Service``, we'll need to export
it. We use Finagle's Http server for this:

.. includecode:: code/quickstart/Server.scala#builder
   :language: scala

The ``serve`` method takes a *bind target* (which port to expose the
server) and the service itself. The server is responsible for
listening for incoming connections, translating the HTTP wire protocol
into ``com.twitter.finagle.http.Request`` objects, and translating
our ``com.twitter.finagle.http.Response`` object back into its wire
format, sending replies back to the client.

The complete server:

.. includecode:: code/quickstart/Server.scala
   :language: scala

We're now ready to run it:

::

	$ ./sbt 'runMain Server'

Which exposes an HTTP server on port 8080 which
dispatches requests to ``service``:

::

	$ curl -D - localhost:8080
	HTTP/1.1 200 OK

Using clients
-------------

In our server example, we define a ``Service`` to respond to requests.
Clients work the other way around: we're given a ``Service`` to *use*. Just as we
exported services with the ``Http.serve``, method, we can *import* them
with a ``Http.newService``, giving us an instance of
``Service[http.Request, http.Response]``:

.. includecode:: code/quickstart/Client.scala#builder
   :language: scala

``client`` is a ``Service`` to which we can dispatch an ``http.Request``
and in return receive a ``Future[http.Response]`` — the promise of an
``http.Response`` (or an error) some time in the future. We furnish
``newService`` with the *target* of the client: the host or set of hosts
to which requests are dispatched.

.. includecode:: code/quickstart/Client.scala#dispatch
   :language: scala

Now that we have ``response``, a ``Future[http.Response]``, we can register
a callback to notify us when the result is ready:

.. includecode:: code/quickstart/Client.scala#callback
   :language: scala

Completing the client:

.. includecode:: code/quickstart/Client.scala
   :language: scala

which in turn is run by:

::

	$ ./sbt 'runMain Client'
	...
	GET success: Response("HTTP/1.1 Status(200)")
	...

Putting it together
-------------------

Now we're ready to create an HTTP proxy! Notice the symmetry above:
servers *provide* a ``Service``, while a client *uses* it. Indeed, an HTTP
proxy can be constructed by just replacing the service we defined in the Server
example with one that was imported with a ``Http.newService``:

.. includecode:: code/quickstart/Proxy.scala
   :language: scala

And we can run it and dispatch requests to it
(be sure to shutdown the Server example from earlier):

::

	$ ./sbt 'runMain Proxy' &
	$ curl --dump-header - --header "Host: twitter.com" localhost:8080
	HTTP/1.1 301 Moved Permanently
	content-length: 0
	date: Wed, 01 Jun 2016 21:26:57 GMT
	location: https://twitter.com/
	...

