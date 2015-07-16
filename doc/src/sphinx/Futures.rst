Concurrent Programming with Futures
===================================

Finagle uses *futures* [#futures]_ to encapsulate and compose
concurrent operations such as network RPCs. Futures are directly
analogous to threads — they provide independent and overlapping
threads of control — and can be thought of as *featherweight
threads*. They are cheap in construction, so the economies of
traditional threads do not apply. It is no problem to maintain
millions of outstanding concurrent operations when they are
represented by futures.

Futures also decouple Finagle from the operating system and runtime
thread schedulers. This is used in important ways; for example,
Finagle uses thread biasing to reduce context switching costs.

.. [#futures] As of version 2.10, Scala has its own implementation
  of Futures, `scala.concurrent.Future`, in part inspired by the one
  from `com.twitter.util`. While `com.twitter.util.Future` will eventually
  implement the same interface as `scala.concurrent.Future`, it does
  not yet — the Future type described presently is `com.twitter.util.Future`.
  
Futures as containers
---------------------

Common examples of operations that are represented
by Futures are:

- an RPC to a remote host
- a long computation in another thread
- reading from disk

Note that these operations are all fallible: remote hosts could
crash, computations might throw an exception, disks could fail, etc.
A `Future[T]`, then, occupies exactly one of three states:

- Empty (pending)
- Succeeded (with a result of type `T`)
- Failed (with a `Throwable`)

While it is possible to directly query this state, this is rarely useful.
Instead, a callback may be registered to receive the results once 
they are made available:

.. code-block:: scala

	val f: Future[Int]
	f.onSuccess { res =>
	  println("The result is " + res)
	}

which will be invoked only on success. Callbacks may also be registered
to account for failures:

.. code-block:: scala

	f.onFailure { cause: Throwable =>
	  println("f failed with " + cause)
	}

Sequential composition
----------------------

Registering callbacks is useful but presents a cumbersome API. The
power of Futures lie in how they *compose*. Most operations can be
broken up into smaller operations which in turn constitute the
*composite operation*. Futures makes it easy to create such composite
operations.

Consider the simple example of fetching a representative thumbnail
from a website (ala Pinterest). This typically involves:

1. Fetching the homepage
2. Parsing that page to find the first image link
3. Fetching the image link

This is an example of *sequential* composition: in order to do the
next step, we must have successfully completed the previous one. With
Futures, this is called `flatMap` [#flatMap]_. The result of `flatMap` is a Future
representing the result of this composite operation. Given some helper
methods — `fetchUrl` fetches the given URL, `findImageUrls` parses an HTML
page to find image links — we can implement our Pinterest-style thumbnail
extract like this:

.. code-block:: scala

	def fetchUrl(url: String): Future[Array[Byte]]
	def findImageUrls(bytes: Array[Byte]): Seq[String]

	val url = "http://www.google.com"

	val f: Future[Array[Byte]] = fetchUrl(url).flatMap { bytes =>
	  val images = findImageUrls(bytes)
	  if (images.isEmpty)
	    Future.exception(new Exception("no image"))
	  else
	    fetchUrl(images(0))
	}

	f.onSuccess { image =>
	  println("Found image of size "+image.size)
	}

`f` represents the *composite* operation. It is the result of first
retrieving the web page, and then the first image link. If either of
the smaller operations fail (the first or second `fetchUrl` or if
`findImageUrls` doesn't successfully find any images), the composite
operation also fails.

The astute reader may have noticed something peculiar: this is
typically the job of the semicolon! That is not far from the truth:
semicolons sequence two statements, and with traditional I/O
operations, have the same effect as `flatMap` does above (the
exception mechanism takes the role of a failed future). Futures
are much more versatile, however, as we'll see.

Concurrent composition
----------------------

It is also possible to compose Futures *concurrently*. We can extend
our above example to demonstrate: let's fetch *all* the images.
Concurrent composition is provided by `Future.collect`:

.. code-block:: scala

	val collected: Future[Seq[Array[Byte]]] =
	  fetchUrl(url).flatMap { bytes =>
	    val fetches = findImageUrls(bytes).map { url => fetchUrl(url) }
	    Future.collect(fetches)
	  }

Here we have combined both concurrent and sequential composition:
first we fetch the web page, then we collect the results of fetching
all of the underlying images.

As with sequential composition, concurrent composition propagates
failures: the future `collected` will fail if any of the underlying
futures do.

It is also simple to write your own combinators that operate over
Futures. This is quite useful, and gives rise to a great amount of
modularity in distributed systems as common patterns can be cleanly
abstracted.

Recovering from failure
-----------------------

Composed futures fail whenever any of their constituent futures
fail. However it is often useful to recover from such failures. The
`rescue` combinator on `Future` is the dual to `flatMap`: whereas `flatMap`
operates over *values*, `rescue` operates over *exceptions*. They 
are otherwise identical. It is often desirable to handle only a subset
of possible exceptions. To accomodate for this `rescue` accepts 
a `PartialFunction`, mapping a `Throwable` to a `Future`:

.. code-block:: scala

	trait Future[A] {
	  ..
	  def rescue[B >: A](f: PartialFunction[Throwable, Future[B]]): Future[B]
	  ..
	}

The following retries a request infinitely should it fail with a
`TimeoutException`:

.. code-block:: scala

	def fetchUrl(url: String): Future[HttpResponse]
	
	def fetchUrlWithRetry(url: String) = 
	  fetchUrl(url).rescue {
	    case exc: TimeoutException => fetchUrlWithRetry(url)
	  }

Other resources
---------------

- `Effective Scala`_ contains a `section discussing futures`_
- As of Scala 2.10, the Scala standard library has its own futures
  implementation and API, described here_. Note that
  this is largely similar to the API used in Finagle
  (*com.twitter.util.Future*), but there are still some naming
  differences.
- Akka_’s documentation also has a `section dedicated to futures`_.

.. _Akka: http://akka.io/
.. _`Effective Scala`: http://twitter.github.com/effectivescala/
.. _`section discussing futures`: http://twitter.github.com/effectivescala/#Twitter's%20standard%20libraries-Futures
.. _here: http://docs.scala-lang.org/overviews/core/futures.html
.. _`section dedicated to futures`: http://doc.akka.io/docs/akka/2.1.0/scala/futures.html

.. [#flatMap] The name `flatMap` may seem strange and unrelated to our present
  discussion, but its etymology is impeccable: it derives from a deeper relationship 
  between the sort of sequential composition we do with futures, to a similar sort 
  of composition we can perform over collections. See the this__ page for more details.
  
__ WikipediaMonads_
.. _WikipediaMonads: http://en.wikipedia.org/wiki/Monad_(functional_programming)

.. TODO
  a section about composing over failures
