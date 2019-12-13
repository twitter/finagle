Twitter Futures
===============

History
~~~~~~~

Futures represent asynchronous values.  Over time, the java community has experimented with a few
different styles of working with them, starting with transforming them to synchronous code, where
you would block until the future was satisfied.  We can see remnants of this in the old
java.util.concurrent.Future interface, where the only way of using the result of a future is
Future#get, which blocks until the future returns.  We can do somewhat better by polling until the
future is satisfied, and doing other things in the mean time, but this is manual.  Instead of
managing it manually, we could use Future#get exclusively, which would force us to use a thread per
concurrent request.

We wanted to try using an evented system instead, so we chose to describe what we want to do after
we get a response, effectively a closure.  This is a common API for asynchronous results, so it's a
natural match for asynchronous values.  Scala had a Futures implementation, `scala.actors.Future`,
but it was tightly coupled to their Actors library, which needed some work.  We ended up working on
our own Futures implementation, which turned into Twitter Futures.  Around the same time, Akka was
working on an actors implementation, and produced their own Futures implementation, which turned
into the `scala.concurrent.Future`.  More recently, Java came out with their own Future which you
can attach closures to, `java.util.concurrent.CompletableFuture`.

Scheduling
~~~~~~~~~~

Each of the modern Futures implementations, Scala, Java, and Twitter, represents a different way of
scheduling the closures that should run on satisfaction.

It's worth noting that our scheduler is configurable (this is also true for Scala Futures, on a
more granular basis), so this model isn't set in stone.  With that said, we often unthinkingly
program against the semantics of the default scheduler, so it would be a significant undertaking to
change the scheduler.

Scala
    Someone else run this.  Typically this means that we schedule the closure to be run in a thread
    pool somewhere.  This has the advantage that there's a fixed cost of satisfying a future per
    closure hanging off of it, which is just the cost of scheduling each closure.  It can also allow
    parallelism if you're doing CPU-intensive work, and is kinder to blocking work.  The
    disadvantage is that it requires doing the work on another thread, and comes with all of the
    costs of passing the message to the other thread.  The way work gets scheduled in Scala can be
    changed on a per-closure basis, but the default is to be backed by a thread pool.

Java
    I'll run this now.  This can be good for caching behavior, and lets us get better stack traces,
    because we can see the causal link between satisfying a Future and running the closures.  The
    disadvantage is that this is no longer stack-safe, because hanging subsequent closures off will
    deepen the stack.  Note that at each point of asynchrony, the stack trace is broken.

Twitter
    I'll run this later.  This still helps with caching, but can be stack-safe.  The disadvantage is
    that the stack traces aren't as useful (in practice we sometimes "run it now").

To illustrate this, imagine that we're doing three pieces of work, doWorkA, doWorkB, doWorkC.

We write it as:
::

    Future.value(doWorkA()).map { a => doWorkB(a) }.map { b => doWorkC(b) }

So what does this turn into at run time?  The answer is different depending upon which model we have for Future.

With Scala, it becomes:

::

    val executor = new ForkJoinPool(2 /* parallelism */)
    val a = doWorkA()
    executor.submit(new Runnable() { def run(): Unit = {
      val b = doWorkB(a)
      executor.submit(new Runnable() { def run(): Unit = doWorkC(b) })
    })

With Java, it becomes:

::

   def workA(): C = {
     val a = doWorkA()
     workB(a)
   }
   def workB(a: A): C = {
     val b = doWorkB(a)
     workC(b)
   }
   def workC(b): C = {
     doWorkC(b)
   }
   workA()

With Twitter, it becomes:

::

   val a = doWorkA()
   val b = doWorkB(a)
   doWorkC(b)

We've chosen this style for performance and for stack safety, and have been quite happy with it
historically.

Interruption
~~~~~~~~~~~~

The other main difference that the different future types have is how they deal with cancellation.
It can be useful to cancel asynchronous work to free up the resources, even though the thread itself
is not blocked, for example if you're consuming resources on a remote host.  Note that this breaks
the model of what a Future is, since it represents an asynchronous value.  Being able to cancel to
prevent work points out that the asynchronous work must be happening somewhere, and asks the future
to keep a reference to where that work is being done, so it can be cancelled.  Scala simply doesn't
implement cancellation, so that if you want to support it, you need to handle it in another way.
Java only supports cancellation by immediately satisfying the future with CancellationException.  If
you want to subsequently cancel the work, you need to ensure that that Future handles the
CancellationException gracefully.  Twitter futures allow users to set interrupt handlers, and can
propagate interruptions up the flatMap chain, so that if a future is no longer needed, it can cancel
the underlying work that will later produce that future.  This means that futures do the right thing
by default, and we don't need to treat each Future#flatMap specially to propagate interrupts.
Interruptions are also advisory for Twitter futures, so that we can choose to ignore them if we
wish.  One nice thing about this style is that it's analogous to the Java interrupt mechanism, where
we in effect set a field that an implementor can choose to respect or not.

Guts
~~~~

There are two main implementations of Twitter future, which are `ConstFuture` and `Promise`.
`ConstFuture` is an already satisfied future–effectively a thin wrapper around a `Try`.  `Promise`
is what most real futures in the wild are, something that starts unsatisfied and gets satisfied
later.  `Promise` uses a state machine, which can be in one of a few different states, Waiting,
Interruptible, Transforming, Interrupted, Linked, and Done.  New `Promises` start in Waiting, and
all of them are unsatisfied except for Done.  You can move from Waiting to Interruptible by calling
`Promise#setInterruptHandler`.  If a `Promise` is constructed with a `Future#transform` method (like
`Future#flatMap`, or `Future#rescue`) it's in Transforming mode.  If a promise is interrupted when
Interruptible, it fires the interrupt handler, and moves to the Interrupted state.  If it's
interrupted when Transforming, it delegates the interrupt to the underlying Future, and moves to the
Interrupted state.  When a future is constructed with a `Future#transform` method and the underlying
future has been satisfied, but the newly constructed one hasn't yet, the `Promise` moves to the
Linked state, where it delegates all work to the new constructed one.  This allows infinite future
recursion without space leaks, which is quite neat.  Scala 2.12 futures have since used our trick to
do it themselves, so they can also do recursion without space leaks.  `Promise#updateIfEmpty` and
its derivatives, like `Promise#setValue`, will move a `Promise` from any state to Done, if they are
not already satisfied, at which point its closures will be submitted to the scheduler to be run by
the calling thread that satisfied the `Promise`.

Detachable
~~~~~~~~~~

When more than one execution dependency depends on the result of a future, it can be dangerous to
interrupt it, since you might inadvertently fail other folks' work.  However, if you never interrupt
it, you can end up with memory leaks if the future is never satisfied and we continuously add work
to it.  Detachable Promises are an attempt to fix this, so that we can mark promises as "may not be
needed later, though the underlying future they depend on will be useful", and must be
constructed explicitly by saying `Promise.attached(underlying)` like `here`_.

.. _here: https://github.com/twitter/finagle/blob/2ae7cdd6124d5d10c9bcbf7c68a0ef2784abf968/finagle-core/src/main/scala/com/twitter/finagle/service/DelayedFactory.scala#L34
