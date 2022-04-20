Threading Model
===============

Similar to other non-blocking event-driven frameworks, Finagle leverages a fixed size thread pool
(I/O worker pool) shared between all clients and servers running within the same JVM process. While
unlocking tremendous scalability potential (less resources are spent to process more events), this
approach comes at the expense of services' responsiveness, being notably sensitive to blocking
operations. Put this way, when blocked, I/O threads can't process the incoming events (new requests,
new connections) thereby degrading the system's overall velocity.

Somewhat conservative defaults for Finagle's worker pool size only exaggerate the problem: there are
few (two per each logical CPU core with the floor of 8 workers) threads in the pool and blocking
even one of them may affect multiple clients and servers. While it's recommended sticking to these
defaults (numerous experiments showed they are good), it's possible to override the worker pool size
with a command line flag.

The following sets the worker pool size to 24.

.. code-block:: scala

    -com.twitter.finagle.netty4.numWorkers=24

I/O Thread Affinity
-------------------

As evident by the flag name, Finagle's I/O threads are allocated and managed within its underlying
networking library, Netty_. Built on top of native non-blocking I/O APIs (epoll_, kqueue_), Netty
dictates an affinity between I/O threads and the list of opened file descriptors (network sockets)
they are watching over. Once a network connection is established, it's assigned a serving I/O thread
that can never be reassigned. This is why blocking even a single I/O thread halts progress in
multiple independent connections (clients, servers).

I/O Threads and User Code
-------------------------

Although not immediately obvious, all users' code that's triggered by receiving a message is run on
Finagle I/O threads, unless explicitly executed somewhere else (e.g., application's own thread pool, a
:ref:`FuturePool <future_pools>`). This includes server's `Service.apply` and all `Future` callbacks
but excludes application startup code (executed in the main thread) and the work triggered by a
timeout expiring (executed in the timer thread).

This design is on par with the asynchronous nature of :doc:`Twitter Futures <developers/Futures>`,
which are purely a coordination mechanism that does not describe any execution environment
(callbacks are run in whatever thread satisfies a `Promise`). Given it's typically I/O threads that
satisfy promises (pending future RPC responses), they are also on the hook for running promises'
callbacks. Sticking with the caller thread reduces context switches at the expense of making I/O
threads vulnerable to users blocking (or just slow) code.

Blocking Examples
-----------------

Not only blocking I/O (e.g., calling a JDBC driver, using the JDK's File API) can block Finagle
threads. CPU intensive computations are equally dangerous: keeping I/O threads busy processing
application-level work means they underserve critical RPC events.

Consider a CPU-bound server application running an algorithm with profoundly bad asymptotic
complexity. Scala's `permutations` operation can serve a good example with its O(n!) worst case
running time. As implemented below, such a server would have a hard time staying responsive as its
I/O threads would be constantly busy (blocked) calculating permutations as opposed to serving
networking events.

.. code-block:: scala

    import com.twitter.finagle.Service
    import com.twitter.finagle.http.{Request, Response}
    import com.twitter.util.Future

    class MyHttpService extends Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        val rep = Response()
        rep.contentString = req.contentString.permutations.mkString("\n")

        Future.value(rep)
      }
    }

A comparably bad thing can happen to clients too. Running `permutations` as part of any callback
(`flatMap`, `onSuccess`, `onFailure`, etc)  on a `Future` returned from a client has the exact same
effect - it saturates an I/O thread.

.. code-block:: scala

    import com.twitter.finagle.Service
    import com.twitter.finagle.http.{Request, Response}

    def process(client: Service[Request, Response]): Future[String] =
      client(Request()).map(rep => rep.contentString.permutations.mkString("\n"))

Identifying Blocking
--------------------

General-purpose JVM profilers and even jstack_ can be quite effective in identifying bottlenecks on
the request path (within I/O threads). There are, however, two metrics that can provide a valuable
insight with no need for external tools.

- `blocking_ms` - a counter of total time spent in `Await.result` and `Await.ready` blocking an I/O
  thread. Refer to `this blog post <https://finagle.github.io/blog/2016/09/01/block-party/>`_  on
  what to do when this counter is not zero.

- `pending_io_events` - a gauge of the number of pending I/O events enqueued in all event loops
  serving this client or server. When this metric climbs up, it indicates I/O queues are clogged
  and I/O threads overloaded.

Whereas getting rid of `Await` on the request path is generally advised, there is no guidance that
could be provided with regards to what is a healthy number of pending I/O events. Clearly, striving
for "zero" or "near zero" might be a reasonable strategy if taken not as the gold standard but a
friendly recommendation. Depending on the workload, even double-digit values could be acceptable for
some applications.

Offloading
----------

Shifting users' work off of I/O threads can go a long way in improving an application's
responsiveness, minding the increase in context switches as well as associated cost of managing
additional JVM threads. However, run your own tests to determine if offloading is good for your
service given its traffic profile and resource allocation.

:ref:`FuturePools <future_pools>` provide a convenient API to wrap any expression with a `Future`
that's scheduled in the underlying ExecutorService_. They come in handy for offloading the I/O
threads in Finagle while preserving the first-class support to Twitter Futures (interrupts, locals).

Offloading can be done on per-method (endpoint) basis:

.. code-block:: scala

    import com.twitter.util.{Future, FuturePool}

    def offloadedPermutations(s: String, pool: FuturePool): Future[String] =
      pool(s.permutations.mkString("\n"))

As well as per entire client or server:

.. code-block:: scala

    import com.twitter.util.FuturePool
    import com.twitter.finagle.Http

    val server: Http.Server = Http.server
      .withExecutionOffloaded(FuturePool.unboundedPool)

    val client: Http.Client = Http.client
      .withExecutionOffloaded(FuturePool.unboundedPool)

Or per entire application (JVM process), using the command-line flag:

.. code-block:: scala

   -com.twitter.finagle.offload.auto=true

Or to enable  with a manually tuned threading configuration:

.. code-block:: scala

   -com.twitter.finagle.offload.numWorkers=14 -com.twitter.finagle.netty4.numWorkers=10

Offload Admission Control
^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: This AC mechanism can only work if global offloading is enabled.

Using system-wide offloading opens the door to an experimental form of admission control. This
admission control mechanism, referred to as Offload AC, uses the behavior of the work queue to
determine when to reject work. The default behavior is to reject work when there is a 20ms wait in
the queue.

This can be enabled using the flag:

.. code-block:: scala

   -com.twitter.finagle.offload.admissionControl=enabled

Or to manually tune the allowable delay:

.. code-block:: scala

   -com.twitter.finagle.offload.admissionControl=50.milliseconds

.. _ExecutorService: https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
.. _jstack: https://docs.oracle.com/javase/7/docs/technotes/tools/share/jstack.html
.. _Netty: https://netty.io/
.. _epoll: https://en.wikipedia.org/wiki/Epoll
.. _kqueue: https://en.wikipedia.org/wiki/Kqueue
