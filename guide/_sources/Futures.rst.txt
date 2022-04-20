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

Synchronized work
-----------------

Synchronization is different from synchronous behavior. Synchronous 
calls wait on some work to complete before executing the next statement 
within the same thread. Synchronized sections, in contrast, allow one
thread to execute statements and block all other callers until the
initial thread is finished with the enclosed statements.

An example that could fail without synchronization would be:

.. code-block:: scala

  def incrementAndReturn(): Integer = {
    counter += 1;
    counter
  }
 
If two threads are executing the `incrementAndReturn` function concurrently
on the same object, it is possible that both execute the statement `counter += 1`
before either executes the return statement. If that happens, both threads would
get the same value (e.g. if `counter` was 45 before either thread ran, 47 would
be returned to both threads).

A simple way to guarantee that each thread sees a unique counter value without
skipping would be to enclose the critical statements in a synchonized block.
Syntactially it looks like this:

.. code-block:: scala

  def incrementAndReturn(): Integer = {
    this.synchronized {
      counter += 1;
      counter
    }
  }

Scoping synchronization
-----------------------

In the previous section a simple introduction and example were given for
synchronization wherein multiple threads have a handle to an object and
can experience unexpected behavior when executing concurrently. The simple
remedy was to add a `synchronized` block on the `this` object within
the function body. What does that mean though?

The syntax for creating a synchronized block requires the user to define a lock
object which grants access to the block that follows. Only one thread at a time
can be in control of a lock object. In the example the `this` object was the
lock, meaning that within the class anywhere a `this.synchronized {...}` block
occurs, it is tied to the same object, `this`. For example:

.. code-block:: scala

  def incrementAndReturn(): Integer = {
    this.synchronized {
      counter += 1;
      counter
    }
  }

  def decrementAndReturn(): Integer = {
    this.synchronized {
      counter -= 1;
      counter
    }
  }

Both functions above are now gated on the same object, `this`, so not only
will multiple threads with the same object handle serially execute
`incrementAndReturn`, they will also be waiting in line for `this` when calling
`decrementAndReturn`.

Other methods within the class could be free to execute without waiting by
omitting the synchronized block

.. code-block:: scala

  def incrementAndReturn(): Integer = {
    this.synchronized {
      counter += 1;
      counter
    }
  }

  def decrementAndReturn(): Integer = {
    this.synchronized {
      counter -= 1;
      counter
    }
  }

  def readCounter(): Integer = {
    counter
  }

Here, any thread may call `readCounter` without waiting to control `this`.

Furthermore, it may be useful for readability and segmenting logic to define
objects whose only purpose is as a synchronization lock rather than blocking
at the granularity of the whole instance. For example:

.. code-block:: scala

  private[this] var counter: Integer = 0
  private[this] val lock: Object = counter

  def incrementAndReturn(): Integer = {
    lock.synchronized {
      counter += 1;
      counter
    }
  }

  def decrementAndReturn(): Integer = {
    lock.synchronized {
      counter -= 1;
      counter
    }
  }

  def readCounter(): Integer = {
    counter
  }

This gives us flexibility as we evolve the class to discover new operations
that need synchronization that can be covered under the umbrella of the `lock`
object. At the moment, the `lock` object is synonymous with the counter itself,
but we may discover some other useful member to use as the `lock` in the future,
or have separate locks for different sets of entangled state.

Synchronization risks
-----------------------

Synchronzation is a very useful language feature to define critical sections
and let the runtime manage blocking, scheduling, and handing off control
between threads. It can be a very efficient technique to ensure predictable
mutation of internal state (like 'counter' above) or other simple actions.

However, synchronization can also expose a developer to a new class of bugs
wherein threads are indefinitely waiting on a data change or on acquiring
the lock object. Two common types of locking problems are livelocks and deadlocks.

A livelock occurs when threads are alive, but the code is waiting for some data
change in order to proceed. The system will wake up a thread, the thread checks
if the data is in the correct state, and then goes back to sleep when it sees no
changes have occurred. If the responsible process or thread is unable to make its
update, the system is in livelock.

A deadlock occurs when two or more threads are mutually blocked/halted on a
statement that is synchronized on a lock object held by the other thread. For
example, imagine a process exists where a thread acquires exclusive access to a
Person, then queries the system for their siblings in order to update them
together. If two threads are each tasked with doing this work for a pair of
siblings, a deadlock can occur. Thread A acquires the lock for Person A. Thread B
acquires the lock for Person B, sibling to Person A. Each now wishes to query
and lock the siblings of their person. Thread A will be waiting for Thread B to
be "finished" with Person B, and Thread B is likewise waiting for Thread A to be
"finished" with Person A. Deadlock. A detailed example of a deadlock_ will follow
below.

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

Parallel composition
--------------------

Collect is specialized for when you do the same operation many times, returning
the same result, and want to know when they're all complete.  However, we often
trigger different operations that we can do in parallel, and may return
different kinds of results.  When we need to use the results of a few different
kinds of computations, it can be useful to wait for all of the results to come
back before continuing.  From a classical thread programming model, the
analogous idea would be calling `join` on a forked thread.  This is where
``Future.join`` comes into play!

There are actually four different modes of ``Future.join``. Although they were
originally written for Scala, the methods on the ``Future`` object also have
Java-friendly versions at ``Futures.join``. The method on the ``Future`` instance
should be usable from Java without any problem.

There's ``Future#join``, which is a method directly on the Future class, which
accepts another Future as an argument and will return a Future that will be
satisfied once both `this` and the argument passed to `join` are satisfied, and
will contain the contents of both Futures.

.. code-block:: scala

  import com.twitter.util.Future

  val numFollowers: Future[Int] = ???
  val profileImageURL: Future[String] = ???

  val userProfileData: Future[(Int, String)] = numFollowers.join(profileImageURL)

There's also ``Future.join``, which can be used for many different results.
There are many ``Future.join`` methods to support many different numbers of
futures that need to be joined.

.. code-block:: scala

  import com.twitter.util.Future

  val numFollowers: Future[Int] = ???
  val profileImageURL: Future[String] = ???
  val followersYouKnow: Future[Seq[User]] = ???

  val userProfileData: Future[(Int, String, Seq[User])] =
    Future.join(numFollowers, profileImageURL, followersYouKnow)

A common thing to do after calling ``Future#join`` is to immediately transform
the result.  As a minor optimization, we can avoid allocating the Tuple2
instance by using ``Future#joinWith``.


.. code-block:: scala

  import com.twitter.util.Future

  val numFollowers: Future[Int] = ???
  val profileImageURL: Future[String] = ???
  val constructUserProfile: (Int, String) => UserProfile

  val userProfile: Future[UserProfile] =
    numFollowers.joinWith(profileImageURL)(constructUserProfile)

The last ``Future.join`` is a bit of an odd one out--like ``Future.collect`` it
operates on a `Seq[Future[A]]`, but it only returns `Future[Unit]` at the
end--namely, whether all of the components pieces succeeded or not. This method
is used for implementing the other ``Future.join`` methods, and is exposed as a
minor optimization for uses cases where all you need to know is success or failure,
and not what the actual results was.

.. code-block:: scala

  import com.twitter.util.Future

  val numFollowers: Future[Int] = ???
  val profileImageURL: Future[String] = ???
  val followersYouKnow: Future[Seq[User]] = ???

  val profileDataIsReady: Future[Unit] =
    Future.join(Seq(numFollowers, profileImageUrl, followersYouKnow))

Synchronization within composition
----------------------------------

.. _deadlock:

As teased above, synchronization can introduce a new class of bugs in
a concurrent environment. A real world example of a deadlock can be found here:
`https://github.com/twitter/util/commit/b3b6... <https://github.com/twitter/util/commit/b3b66cf8df6dd5fb4d97131b110150d5403dfb68>`__

Before the patch, the methods `fail(..)`, `release()`, and the interrupt handler
are all synchronized on `this` while completing a Promise. This can result in
deadlocks if we have two threads interacting with two separate AsyncSemaphores.
Here is a toy example that sets up cross-semaphore interaction. It will look a
bit too obviously-broken to really happen, but isolates the misbehavior that could
reasonably happen by accident:

.. code-block:: scala

  val semaphore1, semaphore2 = new AsyncSemaphore(1)
  // The semaphores have already been taken:
  val permitForSemaphore1 = await(semaphore1.acquire())
  val permitForSemaphore2 = await(semaphore2.acquire())

  // The semaphores have had continuations attached as follows:
  semaphore1.acquire().flatMap { permit =>
    val otherWaiters = semaphore2.numWaiters // synchronizing method
    permit.release()
    otherWaiters
  }

  semaphore2.acquire().flatMap { permit =>
    val otherWaiters = semaphore1.numWaiters // synchronizing method
    permit.release()
    otherWaiters
  }

Now we can trigger a deadlock.

.. code-block:: scala

  val threadOne = new Thread {
    override def run() {
      permitForSemaphore1.release()
    }
  }

  val threadTwo = new Thread {
    override def run() {
      permitForSemaphore2.release()
    }
  }

  threadOne.start
  threadTwo.start

In this situation threadOne and threadTwo may potentially deadlock. It isn’t
obvious from the `release()` calls that this is possible. The reason is the
`acquire()` method returns a Promise and we’ve loaded continuations on them.
When the threads call the `Permit.release()` method the AsyncSemaphore
implementation synchronizes on the lock object (this), and gives the permit to the
next waiting Promise before exiting the synchronized block. That executes the
continuation which calls a method on the other AsyncSemaphore which attempts to
synchronize. This is akin to the descriptive siblings program example above. The
instances `semaphore1` and `semaphore2` aggressively locks their AsyncSemaphore
then block until they can acquire the lock on the other semaphore.

The resolution is presented in the patch linked above, but presented here is a
succinct description of the resolution. Promises provide some methods which, used
together, are similar to `compareAndSet` semantics (a well known, safe pattern).
The new structure of the `release()` method on the Permit:

.. code-block:: scala

  // old implementation
  // def release(): Unit = self.synchronized {
  //  val next = waitq.pollFirst()
  //  if (next != null) next.setValue(this) // <- nogo: still synchronized
  //  else availablePermits += 1
  // }

  // new implementation

  // Here we define a specific lock object, rather than use the `self` of `this`
  // reference. It is synonymous with the internal Queue as that is what is
  // driving our need for synchronization, but using this special-purpose reference
  // gives us an opportunity in the future to refactor more easily.
  private[this] final def lock: Object = waitq

  @tailrec def release(): Unit = {
  // we pass the Promise outside of the lock
  val waiter = lock.synchronized {
    val next = waitq.pollFirst()
    if (next == null) {
      availablePermits += 1
    }
    next
  }

  if (waiter != null) {
    // since we are no longer synchronized with the interrupt handler
    // we leverage the atomic state of the Promise to do the right
    // thing if we race.
    if (!waiter.updateIfEmpty(Return(this))) {
      release()
    }
  }
  
The new implementation is more complex and no longer synchronizes on the interrupt
handler. This exposes the developer to a new consideration; any race between
interrupting the Promise and giving it the Permit.

Using synchronization and Promises together requires a significant amount of care
ensure a program is not at risk of deadlock. Some low risk uses of synchronization are:

* Mutate or access a field.

* Push or pop an element from a private ArrayDeque.

Some examples of risky actions to take in a synchronized block are:

* Calling a function injected by a caller; the function could block, acquire locks of its own, compute pi to a billion digits, etc.

* Calling methods on a trait of unknown origin: this is essentially the same thing as calling a user-injected function.

* Completing a Promise: an example above with dangerous continuations.

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
