Configuration
=============

Prior to :doc:`version 6.0 <changelog>`, the ``ClientBuilder``/``ServerBuilder`` API was the
primary method for configuring the modules inside a Finagle :ref:`client <finagle_clients>`
or :ref:`server <finagle_servers>`. We are moving away from this model for various
:ref:`reasons <configuring_finagle6>`.

The modern way of configuring Finagle clients or servers is to use the Finagle 6 API,
which is generally available via the ``with``-prefixed methods. For example, the following
code snippet creates a new HTTP client altered with two extra parameters: label and transport
verbosity.

.. code-block:: scala

  import com.twitter.finagle.Http

  val client = Http.client
    .withLabel("my-http-client")
    .withTransport.verbose
    .newService("localhost:10000,localhost:10001")

.. note:: All the examples in this user guide use the Finagle 6 API as a main configuration
          method and we encourage all the Finagle users to follow this pattern given that
          the ``ClientBuilder``/``ServerBuilder`` API will eventually be deprecated.
          For help migrating, see :ref:`the FAQ <configuring_finagle6>` as well as
          :ref:`client <finagle_clients>` and :ref:`server <finagle_servers>` configuration.

In addition to ``with``-prefixed methods that provide easy-to-use and safe-to-configure
parameters of Finagle clients and servers, there is an expert-level `Stack API`.

The Stack API is available via the ``configured`` method on a Finagle client/server
that takes a stack param: a value of arbitrary type for which there is an implicit instance
of ``Stack.Param`` type-class available in the scope. For example, the following code
demonstrates how to use ``.configured`` to override TCP socket options provided by default.

.. code-block:: scala

  import com.twitter.finagle.Http
  import com.twitter.transport.Transport

  val client = Http.client
    .configured(Transport.Options(noDelay = false, reuseAddr = false))
    .newService("localhost:10000,localhost:10001")

.. note:: The expert-level API requires deep knowledge of Finagle internals and
          it's highly recommended to avoid it unless you're absolutely sure what
          you're doing.

Design Principles
-----------------

Finagle has many different components and we tried to faithfully model our configuration
to help understand the constituents and the resulting behavior. For the sake of consistency,
the current version of the ``with`` API is designed with the following principles in mind.

1. **Reasonable grouping of parameters**: We target for the fine grained API so most of the
   ``with``-prefixed methods take a single argument. Although, there is a common sense driven
   exception from this rule: some of the parameters only make sense when viewed as a group
   rather than separately (e.g. it makes no sense to specify SOCKS proxy credentials when
   the proxy itself is disabled).

2. **No boolean flags (i.e., enabled, yesOrNo)**: Boolean values are usually hard to
   reason about because their type doesn't encode it's mission in the domain (e.g. in
   most of the cases it's impossible to tell what ``true`` or ``false`` means in a function
   call). Instead of boolean flags, we use a pair of methods ``x`` and ``noX`` to indicate
   whether the ``x`` feature is enabled or disabled.

3. **Less primitive number types in the API**: We promote a sane level of type-full
   programming, where a reasonable level of guarantees is encoded into a type-system.
   Thus, we never use primitive number types for durations and storage units given that
   there are utility types: ``Duration`` and ``StorageUnit``.

4. **No experimental and/or advanced features**: While, it's relatively safe to configure
   parameters exposed via ``with``-prefixed methods, you should never assume the same about
   the `Stack API` (i.e., ``.configured``). It's *easy* to configure basic parameters; it's
   *possible* to configure expert-level parameters.