## Push Based Tools for Protocol Implementers

For a number of protocols messages can be or may even need to be handled as
soon as they become available from the socket. This is commonly true of multiplexed
protocols like Mux and HTTP/2 where the session needs to be constantly reading
in order to receive events like dispatch cancellation and flow control data.

By using a push-based pattern we eliminate the necessary 'read-do-repeat' loop
for these protocols and let the I/O engine feed the session events. This eliminates
a significant layer of overhead caused by the conversion of a push-based I/O
multiplexing subsystem to a pull-based pattern via a buffering mechanism (see
`ChannelTransport` implementations), only to turn it back into a push-based
pattern with a read loop.

### Concurrency Patterns for Push-based Sessions

#### Preferred Concurrency Primitives

The push-based protocol tooling is different from the Transport based tooling
in that events are 'pushed' in both directions, both from the I/O engine into
the `PushSession.receive(..)` method, and from the `PushSession` into the I/O
engine via the `PushChannelHandle.send(..)` methods. Using lock based techniques
can be difficult in this paradigm since the entirety of the operation would
likely happen while holding the lock. Locks also don't play well with Promises
in that satisfying a Promise may trigger arbitrary computations which can result
in deadlocks if this occurs while holding a lock.

A nice solution to the shortfalls of lock-based techniques is to use a serial
executor pattern where the session uses an `Executor` that guarantees both
sequential ordering of submitted work and that there is a happens-before
relationship with respect to work items. This necessarily requires that tasks are
executed one-at-a-time and in the order that there were offered to the `Executor`.
This pattern makes it easy to deal in Futures/Promises while making it simpler to
think about the session state.

#### Naming Contentions

We are using name conventions to add information to fields and methods that should
only be used from within the serial executor. These patterns could just as well
be applied to lock-based concurrency strategies, but we are only introducing them
in the push-based sections of code for now. The strategy comprises of two simple
rules:
- Methods that must only be called from within the serial executor must have the
  prefix `handle`, such as `def handleDontCallMeOutsideTheExecutor() = ???`
- Fields that should only be accessed/mutated from within the serial executor are
  prefixed with `h_`, such as `var h_DontMutateMeOutsideTheExecutor = ???`

These rules make it easier to reason about what concurrency state you're currently
in. For example, if your looking at the definition of a method named `def handleFoo`,
it must be safe to mutate or access a field named `h_fooValue` or call the method
`def handleBar` without entering the serial executor first. Conversely, if you're
looking at the definition of the method `def foo(..)`, you know that you likely must
bounce a call to `def handleFoo` through the serial executor to ensure thread safety
of the session.
