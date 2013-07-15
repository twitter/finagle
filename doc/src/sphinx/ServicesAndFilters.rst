Services & Filters
==================

Services and filters consitute the core abstractions with which
clients and servers are constructed with the Finagle network library.
They are very simple, but also quite versatile. Most of Finagle's
internals are structured around Services and Filters.

Services
--------

A service, at its heart, is a simple function:

::

	trait Service[Req, Rep] extends (Req => Future[Rep])
	
Put another way, a service takes some request of type `Req` and returns
to you a :doc:`Future <Futures>` representing the eventual result (or failure)
of type `Rep`.

Services are used to represent both clients and servers. An *instance*
of a service is used through a client; a server *implements* a `Service`.

To use an HTTP client:

::

	val httpService: Service[HttpRequest, HttpResponse] = ...
	
	httpService(new DefaultHttpRequest(...)) onSuccess { res =>
	  println("received response "+res)
	}
	
or to provide an HTTP server:

::

	val httpService = new Service[HttpRequest, HttpResponse] {
	  def apply(req: HttpRequest) = ...
	}

Services implement *application logic*. You might, for instance,
define a `Service[HttpRequest, HttpResponse]` to implement your
application's external API.

Filters
-------

It is often useful to define *application-agnostic* behavior as well. 
A common example of this is to implement timeouts: if a request
fails to complete within a certain time, the timeout mechanism fails
it with a timeout exception.

Like services, filters are also simple functions:

::

	abstract class Filter[-ReqIn, +RepOut, +ReqOut, -RepIn]
	  extends ((ReqIn, Service[ReqOut, RepIn]) => Future[RepOut])

or: given a request of type `ReqIn` and a service of type
`Service[ReqOut, RepIn]`, return a Future of type `RepOut` (the reply
type). All types are parameterized so that filters may also transform
request or reply types; visualized:

.. xxx
  .. image:: _static/filter.png
  
.. image:: _static/filter2.png 

In most common cases, `ReqIn` is equal to `ReqOut`, and `RepIn` is
equal to `RepOut` — this is in fact sufficiently common to warrant its
own alias:

::

	trait SimpleFilter[Req, Rep] extends Filter[Req, Rep, Req, Rep]

This, then, is a complete definition of a timeout filter:

::

	class TimeoutFilter[Req, Rep](timeout: Duration, timer: Timer)
	    extends SimpleFilter[Req, Rep] 
	{
	  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
	    val res = service(request)
	    res.within(timer, timeout)
	  }
	}

The filter is given a request and the next service in the filter chain.
It then dispatches this request, applying a timeout on the returned
`Future` — `within` is a method on `Future` which applies the given
timeout, failing the future with a timeout exception should it fail
to complete within the given deadline.

Composing filters and services
------------------------------

Filters and services compose with the `andThen` method. For example
to furnish a service with timeout behavior:

::

	val service: Service[HttpRequest, HttpResponse] = ...
	val timeoutFilter = new TimeoutFilter[HttpRequest, HttpResponse](...)
	
	val serviceWithTimeout: Service[HttRequest, HttpResponse] = 
	  timeoutFilter andThen service

Applying a filter to a `Service` produces a new `Service` whose requests
are first filtered through `timeoutFilter`.

We can also compose filters with `andThen`, creating composite filters,
so that

::

	val timeoutFilter = new TimeoutFilter[..](..)
	val retryFilter = new RetryFilter[..](..)
	
	val retryWithTimeoutFilter: Filter[..] =
	  retryFilter andThen timeoutFilter
	  
creates a filter that dispatches requests first through `retryFilter` and
then `timeoutFilter`.
