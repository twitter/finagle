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

The harmony of this analogy has one discordant caveat: *don't
perform blocking operations in a Future*. Futures aren't
preemptive; they must yield control via ``flatMap``. Blocking
operations disrupt this, halting the progress of other asynchronous
operations, and cause your application to experience unexpected
slowness, a decrease in throughput, and potentially deadlocks. But
of course it's possible for blocking operations to be combined safely
with Futures as we'll see.

Blocking or synchronous work
----------------------------

.. _future_pools:

When you have work that is blocking, say I/O or a library
not written in an asynchronous style, you should use a
``com.twitter.util.FuturePool``. FuturePool manages a pool of
threads that don't do any other work, which means that blocking
operations won't halt other asynchronous work.

In the code below, ``someIO`` is an operation that waits for
I/O and returns a string (e.g., reading from a file). Wrapping
``someIO(): String`` in ``FuturePool.unboundedPool`` returns a
``Future[String]``, which allows us to combine this blocking
operation with other Futures in a safe way.

Scala:

.. code-block:: scala

    import com.twitter.util.{Future, FuturePool}

    def someIO(): String =
      // does some blocking I/O and returns a string

    val futureResult: Future[String] = FuturePool.unboundedPool {
      someIO()
    }

Java:

.. code-block:: java

    import com.twitter.util.Future;
    import com.twitter.util.FuturePools;
    import static com.twitter.util.Function.func0;

    Future<String> futureResult = FuturePools.unboundedPool().apply(
      func0(() -> someIO());
    );

Futures as containers
---------------------

Common examples of operations that are represented by futures are:

- an RPC to a remote host
- a long computation in another thread
- reading from disk

Note that these operations are all fallible: remote hosts could
crash, computations might throw an exception, disks could fail, etc.
A ``Future[T]``, then, occupies exactly one of three states:

- Empty (pending)
- Succeeded (with a result of type ``T``)
- Failed (with a ``Throwable``)

While it is possible to directly query this state, this is rarely useful.
Instead, a callback may be registered to receive the results once 
they are made available:

.. code-block:: scala

  import com.twitter.util.Future

  val f: Future[Int] = ???

  f.onSuccess { res: Int =>
    println("The result is " + res)
  }

which will be invoked only on success. Callbacks may also be registered
to account for failures:

.. code-block:: scala

  import com.twitter.util.Future

  val f: Future[Int] = ???

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
Futures, this is called ``flatMap`` [#flatMap]_. The result of ``flatMap`` is a Future
representing the result of this composite operation. Given some helper
methods — ``fetchUrl`` fetches the given URL, ``findImageUrls`` parses an HTML
page to find image links — we can implement our Pinterest-style thumbnail
extract like this:

.. code-block:: scala

  import com.twitter.util.Future

  def fetchUrl(url: String): Future[Array[Byte]] = ???
  def findImageUrls(bytes: Array[Byte]): Seq[String] = ???

  val url = "https://www.google.com"

  val f: Future[Array[Byte]] = fetchUrl(url).flatMap { bytes =>
    val images = findImageUrls(bytes)
    if (images.isEmpty)
      Future.exception(new Exception("no image"))
    else
      fetchUrl(images.head)
  }

  f.onSuccess { image =>
    println("Found image of size " + image.size)
  }

``f`` represents the *composite* operation. It is the result of first
retrieving the web page, and then the first image link. If either of
the smaller operations fail (the first or second ``fetchUrl`` or if
``findImageUrls`` doesn't successfully find any images), the composite
operation also fails.

The astute reader may have noticed something peculiar: this is
typically the job of the semicolon! That is not far from the truth:
semicolons sequence two statements, and with traditional I/O
operations, have the same effect as ``flatMap`` does above (the
exception mechanism takes the role of a failed future). Futures
are much more versatile, however, as we'll see.

Concurrent composition
----------------------

It is also possible to compose Futures *concurrently*. We can extend
our above example to demonstrate: let's fetch *all* the images.
Concurrent composition is provided by ``Future.collect``:

.. code-block:: scala

  import com.twitter.util.Future

  val collected: Future[Seq[Array[Byte]]] =
    fetchUrl(url).flatMap { bytes =>
      val fetches = findImageUrls(bytes).map { url => fetchUrl(url) }
      Future.collect(fetches)
    }

Here we have combined both concurrent and sequential composition:
first we fetch the web page, then we collect the results of fetching
all of the underlying images.

As with sequential composition, concurrent composition propagates
failures: the future ``collected`` will fail if any of the underlying
futures do [#collectToTry]_.

It is also simple to write your own combinators that operate over
Futures. This is quite useful, and gives rise to a great amount of
modularity in distributed systems as common patterns can be cleanly
abstracted.

.. _future_failure:

Recovering from failure
-----------------------

Composed futures fail whenever any of their constituent futures
fail. However it is often useful to recover from such failures. The
``rescue`` combinator on ``Future`` is the dual to ``flatMap``: whereas
``flatMap`` operates over *values*, ``rescue`` operates over *exceptions*. They
are otherwise identical. It is often desirable to handle only a subset
of possible exceptions. To accommodate for this ``rescue`` accepts
a ``PartialFunction``, mapping a ``Throwable`` to a ``Future``:

.. code-block:: scala

  trait Future[A] {
    ..
    def rescue[B >: A](f: PartialFunction[Throwable, Future[B]]): Future[B]
    ..
  }

The following retries a request infinitely should it fail with a
``TimeoutException``:

.. code-block:: scala

  import com.twitter.util.Future
  import com.twitter.finagle.http
  import com.twitter.finagle.TimeoutException

  def fetchUrl(url: String): Future[http.Response] = ???

  def fetchUrlWithRetry(url: String): Future[http.Response] =
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
- `Finagle Block Party`_ details why blocking is bad, and more
  importantly how to detect and fix it.

.. _Akka: https://akka.io/
.. _`Effective Scala`: https://twitter.github.io/effectivescala/
.. _`Finagle Block Party`: https://finagle.github.io/blog/2016/09/01/block-party/
.. _`section discussing futures`: https://twitter.github.io/effectivescala/#Twitter's%20standard%20libraries-Futures
.. _here: https://docs.scala-lang.org/overviews/core/futures.html
.. _`section dedicated to futures`: https://doc.akka.io/docs/akka/2.1.0/scala/futures.html

.. rubric:: Footnotes

.. [#futures] Finagle uses its own ``Future`` implementation by a variety of reasons
   (fewer context switches, interruptibility, support for continuation-local variables,
   tail-call elimination), but mostly because it's preceded SIP-14_ by over a year.

.. [#collectToTry] Use ``Future.collectToTry`` to concurrently collect a sequence of
   futures while accumulating errors instead of failing fast.

.. [#flatMap] The name ``flatMap`` may seem strange and unrelated to our present
   discussion, but its etymology is impeccable: it derives from a deeper relationship
   between the sort of sequential composition we do with futures, to a similar sort of
   composition we can perform over collections. See the this__ page for more details.

__ WikipediaMonads_
.. _WikipediaMonads: https://en.wikipedia.org/wiki/Monad_(functional_programming)

.. _SIP-14: https://docs.scala-lang.org/sips/completed/futures-promises.html

.. TODO
  a section about composing over failures
