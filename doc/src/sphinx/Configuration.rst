Configuration
=============

Clients and Servers
-------------------

.. _finagle6apis:

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

A :ref:`higher-level client API <methodbuilder>`, `MethodBuilder`, builds on
the client, providing logical success rate metrics, application-level retry
policies, per-attempt timeouts, and total timeouts.

.. note:: All the examples in this user guide use the Finagle 6 API as a main configuration
          method and we encourage all the Finagle users to follow this pattern given that
          the ``ClientBuilder``/``ServerBuilder`` API will eventually be deprecated.
          For help migrating, see :ref:`MethodBuilder migration <mb_cb_migration>`,
          :ref:`the FAQ <configuring_finagle6>` as well as
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
          it's recommended to avoid it unless you're sure of what
          you're doing or the corresponding configuration has not yet
          been exposed through ``with``-prefixed methods.

Design Principles
~~~~~~~~~~~~~~~~~

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

.. _toggles:

Feature Toggles
---------------

Feature toggles are a commonly used mechanism for modifying system behavior.
For background, here is a `detailed discussion <https://martinfowler.com/articles/feature-toggles.html>`_
of the topic. As implemented in Finagle they provide a good balance of control between
library and service owners which enables library owners to rollout functionality in a
measured and controlled manner.

Concepts
~~~~~~~~

A :finagle-toggle-src:`Toggle <com/twitter/finagle/toggle/Toggle.scala>` is a total
function from a type-`Int` to `Boolean`. These are used to decide whether a feature is
enabled or not for a given request or service configuration.

A :finagle-toggle-src:`ToggleMap <com/twitter/finagle/toggle/ToggleMap.scala>` is
a collection of `Int`-typed `Toggles`. It provides a means of getting a `Toggle`
for a given an identifier as well as an `Iterator` over the metadata for its `Toggles`.
Various basic implementations exist on the `ToggleMap` companion object.

Usage
~~~~~

If a `Toggle` is on the request path, it is recommended that it be stored in
a member variable to avoid unnecessary overhead on the common path. If the `Toggle` is
used only at startup this is unnecessary.

Here is an example :ref:`Filter <filters>` which uses a `Toggle` on the request path:

.. code-block:: scala

  package com.example.service

  import com.twitter.finagle.{Service, SimpleFilter}
  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.finagle.toggle.{Toggle, ToggleMap}
  import com.twitter.finagle.util.Rng

  class ExampleFilter(
      toggleMap: ToggleMap,
      newBackend: Service[Request, Response])
    extends SimpleFilter[Request, Response] {

    private[this] val useNewBackend: Toggle = toggleMap("com.example.service.UseNewBackend")

    def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
      if (useNewBackend(Rng.threadLocal.nextInt()))
        newBackend(req)
      else
        service(req)
    }
  }

Note that we pass a `ToggleMap` into the constructor and typically this would be
the instance created via
`StandardToggleMap.apply("com.example.service", com.twitter.finagle.stats.DefaultStatsReceiver)`.
This allows for testing of the code with control of whether the `Toggle` is
enabled or disabled by using `ToggleMap.On` or `ToggleMap.Off`. This could have also been
achieved by passing the `Toggle` into the constructor and then using `Toggle.on` or
`Toggle.off` in tests.

Setting Toggle Values
~~~~~~~~~~~~~~~~~~~~~

Library Owners
^^^^^^^^^^^^^^

The base configuration for `StandardToggles` should be defined in a JSON
configuration file at `resources/com/twitter/toggles/configs/$libraryName.json`.
The :finagle-toggle-src:`JSON schema <com/twitter/finagle/toggle/JsonToggleMap.scala>`
allows for descriptions and comments.

Dynamically changing the values across a cluster is specific to a particular deployment
and can be wired in via a
:finagle-toggle-src:`service-loaded ToggleMap <com/twitter/finagle/toggle/ServiceLoadedToggleMap.scala>`.

Deterministic unit tests can be written that modify a `Toggle`\'s settings via
:finagle-toggle-src:`flag overrides <com/twitter/finagle/toggle/flag/overrides.scala>`
using:

.. code-block:: scala

  import com.twitter.finagle.toggle.flag

  flag.overrides.let("your.toggle.id.here", fractionToUse) {
    // code that uses the flag in this block will have the
    // flag's fraction set to `fractionToUse`.
  }

Service Owners
^^^^^^^^^^^^^^

At runtime, the in-process `Toggle` values can be modified using TwitterServer's
"/admin/toggles" `API endpoint
<https://twitter.github.io/twitter-server/Admin.html#admin-toggles>`_.
This provides a quick way to try out a change in a limited fashion.

For setting more permanent `Toggle` values, include a JSON configuration file at
`resources/com/twitter/toggles/configs/$libraryName-service.json`.
The :finagle-toggle-src:`JSON schema <com/twitter/finagle/toggle/JsonToggleMap.scala>`
allows for descriptions and comments.

The JSON configuration also supports optional environment-specific overrides via
files that are examined before the non-environment-specific configs.
These environment-specific configs must be placed at
`resources/com/twitter/toggles/configs/$libraryName-service-$environment.json`
where the `environment` from
:finagle-toggle-src:`ServerInfo.apply() <com/twitter/finagle/server/ServerInfo.scala>`
is used to determine which one to load.

.. _tunables:

Tunables
--------

`Tunables` are a mechanism for service owners to dynamically change configuration
parameters of clients and servers at runtime.

Concepts
~~~~~~~~

A :util-tunable-src:`Tunable <com/twitter/util/tunable/Tunable.scala>` is like a `Function0`;
it produces a value when applied. Dynamic configuration facilitates this value changing
across invocations at runtime.

`Tunables` are accessed by means of a
:util-tunable-src:`TunableMap <com/twitter/util/tunable/TunableMap.scala>`, which contains all the
`Tunables` for a given id (ids are keys for distinguishing `TunableMaps`; each client might
have a separate `TunableMap` that is used for configuration, and the id might be the client label).

Usage
~~~~~

Accessing the `TunableMap` for a given id is done via
:finagle-tunable-src:`StandardTunableMap <com/twitter/finagle/tunable/StandardTunableMap.scala>`,
using `StandardTunableMap.apply("myId")`.
The returned map composes in-memory, service-loaded configurations and local files.

Here is an example of configuring the :src:`TimeoutFilter <com/twitter/finagle/service/TimeoutFilter.scala>`
on an HTTP client with a `Tunable`:

.. code-block:: scala

  package com.example.service

  import com.twitter.finagle.Http
  import com.twitter.finagle.tunable.StandardTunableMap
  import com.twitter.util.Duration
  import com.twitter.util.tunable.{Tunable, TunableMap}

  val clientId = "exampleClient"
  val timeoutTunableId = "com.example.service.Timeout"

  val tunables: TunableMap = StandardTunableMap(clientId)
  val timeoutTunable: Tunable[Duration] =
    tunables(TunableMap.Key[Duration](timeoutTunableId))

  val client = Http.client
    .withLabel(clientId)
    .withRequestTimeout(timeoutTunable)
    .newService("localhost:10000")

Configuration
~~~~~~~~~~~~~
The value of a given `Tunable` is the result of the composition of in-memory, service-loaded
configurations and local files, in that order. If a configuration does not exist
the value from the next configuration is used.

For example, if a server starts up with a file-based configuration for a given id, those values will
be used. If the in-memory configuration is then set, those new values will be used.

In-Memory
^^^^^^^^^
In-memory configuration is provided through a
:util-tunable-src:`TunableMap <com/twitter/util/tunable/TunableMap.scala>`. The Tunable values used
by a given instance can be modified using TwitterServer's `/admin/tunables`
`API endpoint <https://twitter.github.io/twitter-server/Admin.html#admin-tunables>`_.

.. code-block:: scala

  val map = TunableMap.newMutable(source)

File-Based
^^^^^^^^^^

File-based configurations are defined in JSON files with the format specified in
:util-tunable-src:`JsonTunableMapper <com/twitter/util/tunable/JsonTunableMapper.scala>`.

Per-environment and per-instance configurations are supported. Configurations for a given id are
composed from files located at `resources/com/twitter/tunables/`
(ensure that the `resources` directory is properly packaged with your application) in the following
order:

1. $id/$env/instance-$instance.json
2. $id/$env/instances.json
3. $id/instance-$instance.json
4. $id/instances.json

Where $env and $instance are the environment and instance id given by
:finagle-toggle-src:`ServerInfo.apply() <com/twitter/finagle/server/ServerInfo.scala>`.

Service-Loaded
^^^^^^^^^^^^^^
A service-loaded `TunableMap` is loaded through
:util-app-src:`LoadService <com/twitter/app/LoadService.scala>`. For a given id,
`StandardTunableMap` uses `LoadService` to get a
:util-tunable-src:`ServiceLoadedTunableMap <com/twitter/util/tunable/ServiceLoadedTunableMap.scala>`
with a matching id.
