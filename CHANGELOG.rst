.. Author notes: this file is formatted with restructured text
  (http://docutils.sourceforge.net/docs/user/rst/quickstart.html)
  as it is included in Finagle's user's guide.

Note that ``PHAB_ID=#`` and ``RB_ID=#`` correspond to associated messages in commits.

Unreleased
----------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Update Caffeine cache library to version 2.9.2 ``PHAB_ID=D771893``

21.10.0
-------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.loadbalancer.distributor.AddressedFactory` has been removed. Use
`c.t.f.loadbalancer.EndpointFactory` directly instead. ``PHAB_ID=D751145``

* finagle-core: Moved `c.t.finagle.stats.LoadedStatsReceiver` and `c.t.finagle.stats.DefaultStatsReceiver`
  from the finagle-core module to util-stats.  ``PHAB_ID=D763497``

21.9.0
------

New Features
~~~~~~~~~~~~
* finagle-core: Add method Tracing#recordCallSite to record callsite-specific annotations including
  code.function, code.namespace, code.filepath and code.lineno. See details at 
  [OpenTelemetry source code attributes](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/span-general.md#source-code-attributes) ``PHAB_ID=D753929``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-thrift: Removed c.t.finagle.thrift.ThriftClient#newMethodIface and
  ThriftClient#thriftService, use c.t.f.thrift.ThriftClient#methodPerEndpoint. ``PHAB_ID=D747744``

Bug Fixes
~~~~~~~~~~

* finagle-core/partitioning: Close balancers and their gauges when repartitioning.
  ``PHAB_ID=D731675``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Upgrade to Netty 4.1.67.Final and netty-tcnative 2.0.40.Final. ``PHAB_ID=D726343``

* finagle: Downgrade to Netty 4.1.66.Final ``PHAB_ID=D746041``

* finagle: Bump version of Jackson to 2.11.4. ``PHAB_ID=D727879``

* finagle-core: OffloadFilter hands off work from Netty I/O thread to the offload CPU thread pool
  right after we enter the Finagle stack by default. Previously this could be enabled via a toggle.
  The `com.twitter.finagle.OffloadEarly` toggle has been removed. ``PHAB_ID=D733526``

21.8.0 (No 21.7.0 Release)
--------------------------

New Features
~~~~~~~~~~~~

* finagle-mysql: introduce `newRichClient(dest: String, label: String)` method, which removes the
  need for extra boilerplate to convert the destination String to a `c.t.finagle.Name` when
  specifying both `dest` and `label` in String form. ``PHAB_ID=D706140``

* finagle-http, finagle-thriftmux: introduce client.withSni() API. Use this api to specify an
  SNI hostname for TLS clients. ``PHAB_ID=D712652`` 

* finagle-postgresql: introduce `withStatementTimeout`, `withConnectionInitializationCommands`, 
  and `withSessionDefaults` APIs to allow configuring sessions before they're used. 
  ``PHAB_ID=D746525``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Update Caffeine cache library to version 2.9.1 ``PHAB_ID=D660908``

* finagle: Update ScalaCheck to version 1.15.4 ``PHAB_ID=D691691``

* finagle-core: change ServiceClosedException to extend FailureFlags and to be
  universally retryable ``PHAB_ID=710580``

* finagle-http: remove the `com.twitter.finagle.http.UseH2`,
  `com.twitter.finagle.http.UseH2CClients2`, `com.twitter.finagle.http.UseH2CServers` and
  `com.twitter.finagle.http.UseHttp2MultiplexCodecClient` toggles. The configuration for
  `c.t.finagle.Http.client` and `c.t.finagle.Http.server` now default to using the HTTP/2 based
  implementation. To disable this behavior, use `c.t.finagle.Http.client.withNoHttp2` and
  `c.t.finagle.Http.server.withNoHttp2` respectively.

  Alternatively, new GlobalFlag's have been introduced to modify the default behavior of clients
  and servers that have not been explicitly configured, where
  the `com.twitter.finagle.http.defaultClientProtocol`
  and `com.twitter.finagle.http.defaultServerProtocol` flags can be set to `HTTP/1.1` to modify
  the default client or server configuration, respectively. `PHAB_ID=D625880``

* finagle-netty4: Finagle now reuses Netty "boss" (or parent) threads instead of creating a new
  thread per server. Netty parent threads are servicing the server acceptor, a relatively
  lightweight component that listens for new incoming connections before handing them out to the
  global worker pool.  ``PHAB_ID=D662116``

* finagle-http2: introduce optional parameter `NackRstFrameHandling` to enable or disable NACK
  conversion to RST_STREAM frames. ``PHAB_ID=D702696``

* finagle-thrift, finagle-thriftmux: clients may start reporting (correctly) lower success rate.
  Previously server exceptions not declared in IDL were erroneously considered as successes.
  The fgix also improves failure detection and thus nodes previously considered as healthy
  by failure accrual policy may be considered as unhealthy. ``PHAB_ID=D698272``

Bug Fixes
~~~~~~~~~~

* finagle-core: Add `BackupRequestFilter` to client registry when configured. ``PHAB_ID=D686981``

* finagle-thrift, finagle-thriftmux: clients now treat server exceptions
  not declared in IDL as failures, rather than successes,
  and do not skip the configured response classifier for failure accrual.
  ``PHAB_ID=D698272``

21.6.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Introduce `Dtab.limited`, which is a process-local `Dtab` that will
  NOT be remotely broadcast for any protocol, where `Dtab.local` will be
  broadcast for propagation on supported protocols. For path name resolution, the
  `Dtab.local` will take precedence over the `Dtab.limited`, if the same path is
  defined in both, and both take precedence over the `Dtab.base`. The existing
  `Dtab.local` request propagation behavior remains unchanged. ``PHAB_ID=D677860``

* finagle-core: Add descriptions to RequestDraining, PrepFactory, PrepConn, and
  protoTracing modules in StackClient. Add descriptions to preparer and
  protoTracing modules in StackServer. ``PHAB_ID=D685887``

* finagle-mysql: Add support for MySQL 8.0's default `caching_sha2_password` pluggable
  authentication. ``PHAB_ID=D676015``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-memcached: Ketama Partitioned Client has been removed and the Partition Aware
  Memcached Client has been made the default. As part of this change,
  `com.twitter.finagle.memcached.UsePartitioningMemcachedClient` toggle has been removed,
  and it no longer applies. ``PHAB_ID=D661460``

* finagle-core: c.t.f.builder.ServerBuilder has been removed. Use the StackServer interfaces
  instead. ``PHAB_ID=D689067``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Broadcast context keys lookups are now case insensitive. This change is backwards
  compatible as the marshalled key id is unchanged. Although enabled by default, this change will
  be temporarily sitting behind a toggle, `com.twitter.finagle.context.MarshalledContextLookupId`
  that can be used to turn off this change. ``PHAB_ID=D665209``

Deprecations
~~~~~~~~~~~~

* finagle-core: The `ServerBuilder` pattern has been deprecated. Use the stack server pattern
  instead. ``PHAB_ID=D691414``

21.5.0
------

New Features
~~~~~~~~~~~~

* finagle-http2: Added `c.t.f.http2.param.EnforceMaxConcurrentStreams` which allows users to
  configure http2 clients to buffer streams once a connection has hit the max concurrent stream
  limit rather than rejecting them.  A `buffered_streams` gauge has been added to track the
  current number of buffered streams.  ``PHAB_ID=D643138``

* finagle-mux: Added support for TLS snooping to the mux protocol. This allows a thriftmux
  server to start a connection as TLS or follow the existing upgrade pathway at the leisure of
  the client. This also allows the server to support opportunistic TLS and still downgrade to
  vanilla thrift. ``PHAB_ID=D584638``

* finagle-netty4: Added a new counter to keep track of the number of TLS connections that were
  started via snooping. ``PHAB_ID=D667652``

* finagle-thrift: Thrift(Mux) clients and servers now fill in a `c.t.f.Thrift.param.ServiceClass`
  stack param with the runtime class corresponding to a IDL-generated service stub.
  ``PHAB_ID=D676781``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.param.Logger` has been removed. Use external configuration supported by
  your logging backend to alter settings of `com.twitter.finagle` logger.  ``PHAB_ID=D618667``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: Make handling of invalid URI consistent across client implementations. There are
  behavioral inconsistencies amongst the current HTTP client implementations:

  Our HTTP/1.x clients allow for submitting requests that contain non-ASCII characters and
  invalid character encoded sequences, while our HTTP/2 clients will either mangle
  the URI and strip out non-ASCII characters within the Netty pipeline or result in an
  `UnknownChannelException` when attempting to parse invalid character encoded sequences.
  With this change, we now consistently propagate an `InvalidUriException` result, which
  is marked as NonRetryable for all HTTP client implementations. All HTTP server implementations
  maintain behavior of returning a `400 Bad Request` response status, but now also correctly
  handle invalid character encoded sequences. ``PHAB_ID=D660069``

Bug Fixes
~~~~~~~~~~

* finagle-core: Failed writes on Linux due to a remote peer disconnecting should now
  be properly seen as a `c.t.f.ChannelClosedException` instead of a
  `c.t.f.UnknownChannelException`. ``PHAB_ID=D661550``

* finagle-http2: The `streams` gauge is now correctly added for http2 connections over TLS.
  ``PHAB_ID=D643138``

* finagle-core: `c.t.f.n.NameTreeFactory` will now discard empty elements in
  `c.t.f.NameTree.Union's` with zero weight. ``PHAB_ID=D666635``

* finagle-http: All HTTP server implementations consistently return a `400 Bad Request`
  response status when encountering a URI with invalid character encoded sequences.
  ``PHAB_ID=D660069``

21.4.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Introduce a new `ResponseClassifier` ('IgnoreIRTEs') that treats
  `com.twitter.finagle.IndividualRequestTimeoutException`s as `ResponseClass.Ignored`.
  This response classifier is useful when a client has set a super low `RequestTimeout` and
  receiving a response is seen as 'best-effort'. ``PHAB_ID=D645818``

* finagle-mysql: Introduce support of opportunistic TLS to allow mysql clients
  with enabled TLS to speak over encrypted connections with MySQL servers where
  TLS is on, and fallback to plaintext connections if TLS is switched off on
  the server side. ``PHAB_ID=D644982``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: The "failures" counter is changed to be created eagerly, when no failure
  happens, the counter value is 0. ``PHAB_ID=D645590``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-exception: This package was no longer used and therefore has been removed. No
  replacement is planned. ``PHAB_ID=D656591``

21.3.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Added value `ForceWithDtab` to flag
  `-com.twitter.finagle.loadbalancer.exp.apertureEagerConnections` that forces the
  aperture load balancer to eagerly connect, even in staging environments where
  Dtab locals are set. ``PHAB_ID=D613989``

* finagle-core: Introduce a new `Backoff` to create backoffs based on varies strategies, where
  backoffs are calculated on the fly, instead of being created once and memoized in a `Stream`.
  Also introduced `Backoff.fromStream(Stream)` and `Backoff.toStream` to help with migration to
  the new API. ``PHAB_ID=D592562``

* finagle-netty4: Upgrade to Netty 4.1.59.Final and TcNative 2.0.35.Final. ``PHAB_ID=D629268``

* finagle-http: Integrate Kerberos authentication filter to finagle http client and server.
  ``PHAB_ID=D634270`` ``PHAB_ID=D621714``

* finagle-core: Provided `c.t.f.ssl.TrustCredentials.X509Certificates` to enable directly
  passing `X509Certificate` instead of passing a `File`. ``PHAB_ID=D641088``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: Builds are now only supported for Scala 2.12+ ``PHAB_ID=D631091``

* finagle-base-http: Kerberos jaas config `KerberosConfiguration` is replaced with ServerKerberosConfiguration
  and ClientKerberosConfiguration concrete classes. ``PHAB_ID=D634270``

* finagle-core: Changed flag `-com.twitter.finagle.loadbalancer.exp.apertureEagerConnections"
  from having Boolean values true or false to `EagerConnectionsType` values `Enable`,
  `Disable`, and `ForceWithDtab`. ``PHAB_ID=D613989``

* finagle-mysql: The constructor of `c.t.f.mysql.transport.MysqlBufReader` now takes an underlying
  `c.t.io.ByteReader`. Prior uses of the constructor, which took a `c.t.io.Buf`, should migrate to
  using `c.t.f.mysql.transport.MysqlBufReader.apply` instead. ``PHAB_ID=D622705``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Revert to scala version 2.12.12 due to https://github.com/scoverage/sbt-scoverage/issues/319
  ``PHAB_ID=D635917``

* finagle: Bump scala version to 2.12.13 ``PHAB_ID=D632567``

* finagle-core: Move helper tracing methods like `traceLocal` in `Trace` into the `Tracing` class. This
  allows cheaper use of these APIs by first capturing a Trace via `Trace#apply`, avoiding the extra lookups
  that will add overhead on the request path. ``PHAB_ID=D633318``.

* finagle-core: `c.t.finagle.InetResolver`, `c.t.finagle.builder.ClientBuilder`,
  `c.t.finagle.liveness.FailureAccrualFactory`, `c.t.finagle.liveness.FailureAccrualPolicy`,
  `c.t.finagle.param.ClientParams`, `c.t.finagle.param.SessionQualificationParams`,
  `c.t.finagle.service.FailFastFactory`, `c.t.finagle.service.RequeueFilter`,
  `c.t.finagle.service.Retries`, `c.t.finagle.service.RetryFilter`, and
  `c.t.finagle.service.RetryPolicy` will accept the new `c.t.finagle.service.Backoff` to create
  backoffs. Services can convert a `Stream` to/from a `Backoff` with `Backoff.fromStream(Stream)`
  and `Backoff.toStream`. ``PHAB_ID=D592562``

* finagle-core: remove the `com.twitter.finagle.loadbalancer.apertureEagerConnections` Toggle and
  change the default behavior to enable eager connections for `c.t.f.loadbalancer.ApertureLeastLoaded`
  and `c.t.f.loadbalancer.AperturePeakEwma` load balancers. The state of the
  `com.twitter.finagle.loadbalancer.apertureEagerConnections` GlobalFlag now also defaults to enable
  this feature (`Enable`. You can disable this feature for all clients via setting the
  `com.twitter.finagle.loadbalancer.apertureEagerConnections` GlobalFlag to `Disable` for your process.
  (i.e. `-com.twitter.finagle.loadbalancer.apertureEagerConnections=Disable`).
  ``PHAB_ID=D625618``

* finagle-partitioning: Add EndpointMarkedDeadException. Before this change, the exception being
  thrown appeared as an anonymous class and it made deciphering it difficult when it came up in
  stats. Create a concrete class and throw that. ``PHAB_ID=D640835``

Deprecations
~~~~~~~~~~~~
* finagle-core: `Backoff.fromJava` is marked as deprecated, since the new `Backoff` is java-friendly.
  For services using `Stream.iterator` on the old `Backoff`, please use the new API
  `Backoff.toJavaIterator` to acquire a java-friendly iterator. ``PHAB_ID=D592562``


21.2.0
------

New Features
~~~~~~~~~~~~

* finagle-zipkin-core: Record `zipkin.sampling_rate` annotation to track sampling
  rate at trace roots. ``PHAB_ID=D601379``

* finagle-core: Added variant of `c.t.f.Address.ServiceFactory.apply` that does not require
  specifying `c.t.f.Addr.Metadata` and defaults to `c.t.f.Addr.Metadata.empty`. ``PHAB_ID=D605438``

* finagle-core: Added variant of `c.t.f.Name.bound` which takes a `c.t.f.Service` as a parameter.
  Tying a `Name` directly to a `Service` can be extremely useful for testing the functionality
  of a Finagle client. ``PHAB_ID=D605745``

* finagle-mux: Added variant of `c.t.f.mux.Request.apply` and `c.t.f.mux.Requests.make` which takes
  only the body of the `Request` (in the form of `c.t.io.Buf`) as a parameter. This is useful for
  when the path value of a `Request` is not used by the server (e.g. testing). ``PHAB_ID=D613686``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-memcached: The log level of messages pertaining to whether a Memcached client is using the
  older non-partitioned or the newer partitioned version has been lowered. These messages are no
  longer written at an 'info' level. ``PHAB_ID=D607487``

21.1.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Add `clnt/<FilterName>_rejected` annotation to filters that may throttle requests,
  including `c.t.finagle.filter.NackAdmissionFilter` and `c.t.finagle.filter.RequestSemaphoreFilter`.
  ``PHAB_ID=D597875``

* finagle-http: Record http-specific annotations including `http.status_code` and
  `http.method`. See details at
  https://github.com/open-telemetry/opentelemetry-specification/tree/master/specification/trace
  ``PHAB_ID=D580894``

Bug Fixes
~~~~~~~~~

* finagle-core: Fix wraparound bug in `Ring.weight`, as reported by @nvartolomei ``PHAB_ID=D575958``

* finagle-mysql: Update the UTF8 character set to cover those added in MySQL 8.
  ``PHAB_ID=D590996``

* finagle-thriftmux: Fixed a bug where connections were not established eagerly in ThriftMux
  MethodBuilder even when eager connections was enabled. ``PHAB_ID=D589592``


Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

 * finagle-mysql: Don't use the full query when adding tracing annotations. ``PHAB_ID=D593944``

20.12.0
-------

New Features
~~~~~~~~~~~~

* finagle-benchmark: Add a benchmark for LocalContext. ``PHAB_ID=D588632``

* finagle-core: Add a new filter, `ClientExceptionTracingFilter`, that records error annotations for
  completed spans. Annotations include `error`, `exception.type`, and `exception.message`.
  See https://github.com/open-telemetry/opentelemetry-specification for naming details.
  ``PHAB_ID=D583001``

* finagle-core: Add a new stat (histogram) that reports how long a task has been sitting in the
  offload queue. This instrumentation is sampled at the given interval (100ms by default) that
  can be overridden with a global flag `com.twitter.finagle.offload.statsSampleInterval`.
  ``PHAB_ID=D571980``

* finagle-core: Add a new experimental flag `com.twitter.finagle.offload.queueSize` that allows to
  put bounds on the offload queue. Any excess work that can't be offloaded due to a queue overflow
  is run on IO (Netty) thread instead. Put this way, this flag enables the simplest form of
  backpressure on the link between Netty and OffloadFilter. ``PHAB_ID=D573328``

* finagle-netty4: Add `ExternalClientEngineFactory` to the open source version of Finagle. This
  `SslClientEngineFactory` acts as a better example of how to build custom client and server engine
  factories in order to reuse SSL contexts for performance concerns. ``PHAB_ID=D572567``

* finagle-core: Provide `com.twitter.finagle.naming.DisplayBoundName` for configuring how to
  display the bound `Name` for a given client in metrics metadata. ``PHAB_ID=D573905``

* finagle-core: Provide `ClientParamsInjector`, a class that will be service-loaded at run-time
  by Finagle clients, and will allow generic configuration of all sets of parameters.
  ``PHAB_ID=D574861``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Move `DarkTrafficFilter` and `AbstractDarkTrafficFilter` from the experimental
  finagle-exp to supported finagle-core. The package containing these classes changed from
  `c.t.finagle.exp` to `c.t.finagle.filter`. ``PHAB_ID=D572384``

* finagle-core, finagle-thrift: Move `ForwardingWarmUpFilter` and `ThriftForwardingWarmUpFilter`
  from the experimental finagle-exp to supported finagle-core, and finagle-thrift, respectively.
  The package containing `ForwardingWarmUpFilter` changed from `c.t.finagle.exp` to
  `c.t.finagle.filter`, and the package containing `ThriftForwardingWarmUpFilter` changed from
  `c.t.finagle.exp` to `c.t.finagle.thrift.filter`. ``PHAB_ID=D573545``

* finagle-core: `FailureAccrualFactory.isSuccess` has been replaced with the method
  `def classify(ReqRep): ResponseClass` to allow expressing that a failure should be ignored.
  ``PHAB_ID=D571093``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Use Scala default implementation to calculate Hashcode and equals method for
  ServiceFactoryProxy. ``PHAB_ID=D569045``

* finagle: Update build.sbt to get aarch64 binaries and try the fast path acquire up to 5 times
  before failing over to the AbstractQueuedSynchronizer slow path in NonReentrantReadWriteLock
  for Arm64. ``PHAB_ID=D589167``

Bug Fixes
~~~~~~~~~

* finagle-core: Users should no longer see the problematic
  `java.lang.UnsupportedOperationException: tail of empty stream` when a `c.t.f.s.RetryPolicy`
  is converted to a String for showing. ``PHAB_ID=D582199``

20.10.0
-------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-thrift: Change the partition locator function getLogicalPartitionId in
  PartitioningStrategy from Int => Int to Int => Seq[Int], which supports many to many mapping
  from hosts and logical partitions. ``PHAB_ID=D550789``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Disable eager connections for balancers with a non 1.0 weight. ``PHAB_ID=D567842``

20.9.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Add RelativeName field to Metric Metadata and populate it for
  client and server metrics. ``PHAB_ID=D552357``

* finagle-scribe: Add `c.t.finagle.scribe.Publisher` for publishing messages to a
  Scribe process. ``PHAB_ID=D539003``

* finagle-thrift/partitioning: Support dynamic resharding for partition aware thriftmux client.
  ``PHAB_ID=D543466``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Bump version of Jackson to 2.11.2. ``PHAB_ID=D538440``

Bug Fixes
~~~~~~~~~

* finagle-core: The TraceId alternative constructor now forwards the `traceIdHigh` parameter to
  the primary constructor. ``PHAB_ID=D546612``

* finagle-core: Enforce ordering in RequestLogger to make sure we log the end of async
  action before higher modules have a chance to process the result. ``PHAB_ID=D551741``

* finagle-stats: Handle Double percentile rounding error in stat format. ``PHAB_ID=D554778``

20.8.1
------

New Features
~~~~~~~~~~~~

* finagle-core: Populate SourceRole field of Metric Metadata for client and server metrics.
  ``PHAB_ID=D542596``
* finagle-thriftmux: Add MethodBuilder specific APIs for ThriftMux partition aware client.
  ``PHAB_ID=D531900``

* finagle-netty4: Upgrade to Netty 4.1.51.Final and TcNative 2.0.34.Final. ``PHAB_ID=D536904``

20.8.0 (DO NOT USE)
-------------------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-netty4-http: Post, Put, Patch non-streaming outbound requests with empty bodies will
  be added the `Content-Length` header with value `0`. ``PHAB_ID=D518010``

* finagle-core: A ServiceFactory created by ServiceFactory.const/constant propagates the wrapped
  service status. ``PHAB_ID=D520598``

* finagle-core: Only deposit into the RetryBudget after a request succeeds.
  This should help mitigate retry storm behavior. ``PHAB_ID=D528880``

* finagle-http: `c.t.f.http.filter.PayloadSizeFilter` no longer adds an annotation on each
  streaming chunk and instead aggregates the byte count and adds a single record on stream
  termination. ``PHAB_ID=D522543``

* finagle-zipkin-scribe: zipkin scribe `log_span` prefix replaced with `scribe`. `zipkin-scribe/scribe/<stats>`. ``PHAB_ID=D527531``

New Features
~~~~~~~~~~~~

* finagle-core: introduce type-safe `ReqRep` variant ``PHAB_ID=D520027``

* finagle-core: Added a new variant of `Filter.andThenIf` which allows passing the parameters
  as individual parameters instead of a Scala tuple. ``PHAB_ID=D523010``

* finagle-core: new metric (counter) for traces that are sampled. `finagle/tracing/sampled` ``PHAB_ID=D522355``

* finagle-netty4: Add the `c.t.f.netty4.Netty4Listener.MaxConnections` param that can be used
  to limit the number of connections that a listener will maintain. Connections that exceed
  the limit are eagerly closed. ``PHAB_ID=D517737``

* finagle-thrift: Added java-friendly `c.t.f.thrift.exp.partitioning.ClientHashingStrategy` and
  `c.t.f.thrift.exp.partitioning.ClientCustomStrategy` `create` methods, and added java-friendly
  `c.t.f.thrift.exp.partitioning.RequestMergerRegistry#addRequestMerger` and
  `c.t.f.thrift.exp.partitioning.ResponseMergerRegistry#addResponseMerger` to make partitioning
  easier to use from Java. ``PHAB_ID=D525770``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `ReqRep` can no longer be created via `new ReqRep(..)`. Please use
  `ReqRep.apply(..)` instead.
  ``PHAB_ID=D520027``

* finagle-thrift: Updated the `c.t.f.thrift.exp.partitioning.ClientHashingStrategy` and the
  `c.t.f.thrift.exp.partitioning.ClientCustomStrategy` to take constructor arguments instead
  of needing to override methods on construction. ``PHAB_ID=D525770``

* finagle-zipkin-core: Removed unused `statsReceiver` constructor argument from `RawZipkinTracer`. ``PHAB_ID=D527531``

20.7.0
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Correct the spelling of `Tracing.recordClientSendFrargmet()` to
  `Tracing.recordClientSendFragment()` ``PHAB_ID=D505617``

* finagle-redis: Use `StrictKeyCommand` for XDEL ``PHAB_ID=D517291``

* finagle-toggle: `Toggle.isDefinedAt(i: Int)` has become `Toggle.isDefined`. Additionally, a new method `Toggle.isUndefined` has been added. ``PHAB_ID=D516868``

* finagle-zipkin-scribe: The scribe.thrift file was moved to finagle-thrift/src/main/thrift under a new
  namespace. `com.twitter.finagle.thrift.scribe.(thrift|thriftscala)` ``PHAB_ID=D511471``

Bug Fixes
~~~~~~~~~

* finagle-zipkin-scribe: The scribe client should be configured using the `NullTracer`. Otherwise, spans
  produced by the client stack will be sampled at `initialSampleRate`. ``PHAB_ID=D507318``

* finagle-redis: The redis client now includes the `RedisTracingFilter` and `RedisLoggingFilter` by default.
  Previously, the filters existed but were not applied to the client or accessible. ``PHAB_ID=D558552``

20.6.0
------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: FailFastFactory is now disabled at runtime when a client's destination has only
  one endpoint, since the client cannot do anything meaningful by breaking the circuit early.
  This is recommended as a best practice anyway, now it's the default behavior. Less things
  to configure and worry about! ``PHAB_ID=D498911``

* finagle-core: namer annotations are prefixed with "clnt/". ``PHAB_ID=D492443``

* finagle-core: `namer.success` & `namer.failure` are not annotated as they are not request based.
  `namer.tree` annotation was also removed to reduce the size of traces.``PHAB_ID=D492443``

* finagle-core: The offload filter client annotation is annotated under the child request span instead of
  its parent. The offload filter annotations are also changed to be binary annotations with the key
  `(clnt|srv)/finagle.offload_pool_size` and the value being the pool size ``PHAB_ID=D502521``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-mux: The `c.t.f.mux.transport.OpportunisticTlsConfig` params were moved to
  `c.t.f.ssl.OpportunisticTlsConfig`. ``PHAB_ID=D482693``

* finagle-core: Migrated `List[Tracer]` to `Seq[Tracer]` in `Tracing`, and `tracersCtx`.
  ``PHAB_ID=D489697``

* finagle-core: `PayloadSizeFilter` and `WireTracingFilter` are now public APIs.
  ``PHAB_ID=D493803``

* finagle-zipkin-core: `initialSampleRate` flag will now fail if the sample rate is not in the range
  [0.0, 1.0]. ``PHAB_ID=D498408``

* finagle-mysql: mysql client annos are prefixed with `clnt/` ``PHAB_ID=D492274``

New Features
~~~~~~~~~~~~

* finagle-thrift: Expose `c.t.f.thrift.exp.partitioning.PartitioningStrategy`,
  the bundled PartitioningStrategy APIs are public for experiments.
  ``PHAB_ID=D503436``

* finagle-http: Add LoadBalancedHostFilter to allow setting host header after LoadBalancer
  ``PHAB_ID=D498954``

* finagle-core: Trace the request's protocol identified by the `ProtocolLibrary` of the client
  stack. This is annotated under `clnt/finagle.protocol`. ``PHAB_ID=D495645``

* finagle-core: Add `letTracers` to allow setting multiple tracers onto the tracer stack.
  ``PHAB_ID=D489697``

* finagle-core: `DeadlineFilter` now exposes a metric `admission_control/deadline/remaining_ms`
  which tracks the remaining time in non-expired deadlines on the server side. An increase in this
  stat, assuming request latency is constant and timeout configurations upstream have not changed,
  may indicate that upstream services have become slower. ``PHAB_ID=D492608``

* finagle-redis: Make partitionedClient accessible. ``PHAB_ID=D492754``

* finagle-core, finagle-http, finagle-thriftmux: introduce `MethodBuilder` `maxRetries`
  configuration. A ThriftMux or HTTP method can now be configured to allow a specific number of
  maximum retries per request, where the retries are gated by the configured `RetryBudget`. This
  configuration can be applied via `Http.client.methodBuilder(name).withMaxRetries(n)` or
  `ThriftMux.client.methodBuilder(name).withMaxRetries(n)`. ``PHAB_ID=D493139``

* finagle-memcached: Annotate the shard id of the backend the request will reach. ``PHAB_ID=D491738``

Bug Fixes
~~~~~~~~~

* finagle-zipkin-core: Remove flush and late-arrival annotations, which artificially extend
  trace durations. ``PHAB_ID=D498073``

* finagle-core: namer annotations are added at the Service level instead of ServiceFactory as
  traces are intended to be request based ``PHAB_ID=D492443``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-memcached: The key in `RetrievalCommand` are ommited in traces. The total number of Hits
  and Misses are annotated via a counter instead under `clnt/memcached.(hits/misses)` ``PHAB_ID=D491738``

20.5.0
------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Bump jackson version to 2.11.0. ``PHAB_ID=D457496``

20.4.1
------

New Features
~~~~~~~~~~~~

* finagle-redis: Add `ConnectionInitCommand` stack to set database and password.
  ``PHAB_ID=D468835``

* finagle-mysql: Add `ConnectionInitSql` stack to set connection init sql. ``PHAB_ID=D468856``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Requeued reqeuests due to the `c.t.finagle.service.RequeueFilter` will generate
  their own spanId. ``PHAB_ID=D459106``

Bug Fixes
~~~~~~~~~

* finagle-core: Restrict `OffloadFilter` from allowing interruption of the work performed in
  the worker pool. This is to ensure that the worker thread isn't interruptible, which is a
  behavior of certain `FuturePool` implementations. ``PHAB_ID=D465042`` ``PHAB_ID=D465591``

* finagle-netty4: ChannelStatsHandler will now only count the first channel `close(..)` call
  when incrementing the `closes` counter. ``PHAB_ID=D462360``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-toggle: Removed abstract type for `c.t.finagle.Toggle`, all Toggles are of type `Int`.
  This is to avoid Integer auto-boxing when calling `Toggle.apply`, thus to improve overall toggle
  performance. ``PHAB_ID=D456960``

* finagle-core: Retried requests due to the `c.t.finagle.service.RetryFilter` will generate their
own spanId. ``PHAB_ID=`D466083`

20.4.0 (DO NOT USE)
-------------------

New Features
~~~~~~~~~~~~

* finagle-core: Add `Transport.sslSessionInfo` local context which provides access to
  the `SSLSession`, session id, cipher suite, and local and peer certificates.
  ``PHAB_ID=D459854``

* finagle-thrift/thriftmux: Thrift and ThriftMux client side can set a sharable
  TReusableBuffer by `withTReusableBufferFactory`. ``PHAB_ID=D452763``

* finagle: Server side TLS snooping has been added to all server implementations with the
  exception of Mux/ThriftMux. This feature allows a server to select between a cleartext
  and TLS connection based on identifying the initial bytes as a TLS record of type handshake.
  ``PHAB_ID=D436225``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-partitioning: Rename `c.t.finagle.partitioning.CacheNode` and `CacheNodeMetadata`
  to `c.t.finagle.partitioning.PartitionNode` and `PartitionNodeMetadata`. ``PHAB_ID=D448015``

* finagle-partitioning: Rename `c.t.finagle.partitioning.KetamaClientKey` to `HashNodeKey`
  ``PHAB_ID=D449929``

Bug Fixes
~~~~~~~~~

* finagle-base-http: RequestBuilder headers use SortedMap to equalize keys in different caps.
  `setHeader` keys are case insensitive, the last one wins. ``PHAB_ID=D449255``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-stats: JsonExporter now caches the regex matching, so that you only need to check
  the result of regex matching on new stats. ``PHAB_ID=D459391``

20.3.0
------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-netty4: When not using the JDK implementation, the Netty reference counted SSL
  types are used which move SSL cleanup out of the GC cycle, reducing pause durations.
  ``PHAB_ID=D442409``

* finagle-netty4: Upgraded to Netty 4.1.47.Finale and netty-tcnative 2.0.29.Final. ``PHAB_ID=D444065``

Bug Fixes
~~~~~~~~~

* finagle-zipkin-scribe: add a logical retry mechanism to scribe's TRY_LATER response ``PHAB_ID=D441366``

* finagle-zipkin-scribe: scope logical stats under "logical" ``PHAB_ID=D445075``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-zipkin-scribe: update the deprecated `FutureIface` to `MethodPerEndpoint` ``PHAB_ID=D441366``

* finagle-core: Removed `c.t.finagle.service.ShardingService`. ``PHAB_ID=D445176``

20.2.1
------

New Features
~~~~~~~~~~~~

* finagle-opencensus-tracing: Add support for providing a custom TextFormat for header
  propagation. ``PHAB_ID=D432003``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: Support for the `SameSite` cookie attribute is now on by default. This can
  be manipulated via the `com.twitter.finagle.http.cookie.supportSameSiteCodec` flag. This means
  that cookies that have a value other than `Unset` for the `sameSite` field will have the
  attribute encoded (by servers) and decoded (by clients). See this
  [Chromium blog post](https://blog.chromium.org/2019/10/developers-get-ready-for-new.html)
  for more information about the `SameSite` attribute. ``PHAB_ID=D426349``

* finagle-core: The default NullTracer for ClientBuilder has been removed. Affected clients may
  now see tracing enabled by default via the Java ServiceLoader, as described in the
  [Finagle User's Guide](http://twitter.github.io/finagle/guide/Tracing.html). ``PHAB_ID=D437948``

* finagle-http2: The HTTP/2 frame logging tools now log at level INFO. This is symmetric with
  the behavior of the `ChannelSnooper` tooling which serves a similar purpose which is to aid
  in debugging data flow and isn't intended to be enabled in production. ``PHAB_ID=D441876``

Bug Fixes
~~~~~~~~~

* finagle-http2: Initialize state in H2Pool before use in the gauge to avoid a
  NullPointerException. ``PHAB_ID=D428272``

* finagle-http2: HTTP/2 server pipeline now traps close calls to ensure that
  events from the initial HTTP/1.x pipeline don't close the HTTP/2 session. For
  example, the initial pipeline was subject to session timeouts even though the
  tail of the socket pipeline was effectively dead. Closing of HTTP/2 server
  pipelines is now handled through the `H2ServerFilter`. ``PHAB_ID=D429554``

* finagle-http2: HTTP/2 servers clean out unused channel handlers when upgrading
  from a HTTP/1.x pipeline, removing some traps such as unintended timeouts.
  ``PHAB_ID=D429416``

* finagle-opencensus-tracing: Fixed internal server error when invalid or no propagation headers
  are provided. ``PHAB_ID=D432003``

* finagle-zipkin-scribe: export application metrics under a consistent `zipkin-scribe` scope. Finagle client
  stats under `clnt/zipkin-scribe` ``PHAB_ID=D432274``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-zipkin-scribe: Coalesce `ScribeRawZipkinTracer` apply methods into two simple ones. ``PHAB_ID=D432274``

* finagle-zipkin-scribe: `DefaultSampler` moved to `c.t.f.zipkin.core` in finagle-zipkin-core. ``PHAB_ID=D439456``

* finagle-zipkin-scribe: `initialSampleRate` GlobalFlag is moved to finagle-zipkin-core, under the same package
  scope `c.t.f.zipkin`. ``PHAB_ID=D439456``

20.1.0
------

New Features
~~~~~~~~~~~~
* finagle-memcached: Upgrade to Bijection 0.9.7. ``PHAB_ID=D426488``

* finagle-opencensus-tracing: Enables cross-build for 2.13.0. ``PHAB_ID=D421452``

* finagle-thriftmux: Add support for automatically negotiating compression between a client
  and server.  Off by default, clients and servers must be configured to negotiate.
  ``PHAB_ID=D414638``

* finagle-stats: Enables cross-build for 2.13.0. ``PHAB_ID=D421449``

* finagle-stats-core: Enables cross-build for 2.13.0. ``PHAB_ID=D421449``

* finagle-serversets: Add generic metadata support in ServerSet. Add support for announcing the
  generic metadata via ZkAnnouncer. Add support to resolve the generic metadata via Zk2Resolver
  ``PHAB_ID=D421151``

* finagle-serversets: Add support for announcing additional endpoints via ZkAnnouncer
  ``PHAB_ID=D431062``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-partitioning: `ZKMetadata` case class has a new default argument breaking API for
  Java users. ``PHAB_ID=D421151``

* finagle-serversets: `Endpoint` case class has a new metadata argument. ``PHAB_ID=D421151``

Bug Fixes
~~~~~~~~~

* finagle-memcached: MemcachedTracingFilter should replace StackClient.Role.protoTracing and not
  the protocol-agnostic ClientTracingFilter ``PHAB=D427660``

19.12.0
-------

New Features
~~~~~~~~~~~~

* finagle-core, finagle-exp: Add annotations to ``DarkTrafficFilter`` to identify which span
  is dark, as well as which light span it correlates with. ``PHAB_ID=D402864``

* finagle-core: Introduce `Trace#traceLocal` for creating local spans within a trace context.
  ``PHAB_ID=D404869``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Upgrade to jackson 2.9.10 and jackson-databind 2.9.10.1 ``PHAB_ID=D410846``

* finagle-core: Per-method metrics on MethodBuilder are now created lazily, so if you have
  methods that you don't use, the associated metrics won't be exported.  ``PHAB_ID=D400382``

* finagle-mysql: The RollbackFactory no longer attempts to roll back if the underlying
  session is closed since it is highly unlikely to succeed. It now simply poisons the
  session and calls close. ``PHAB_ID=D408155``

* finagle-netty4: Change the 'connection_requests' metric to debug verbosity.
  ``PHAB_ID=D391289``

* finagle-serversets: Ensure `ZkSession#retrying` is resilient to ZK host resolution failure.
  ``PHAB_ID=D403895``

* finagle-thrift: Per-method metrics are now created lazily, so if you have methods on a Thrift
  service that you don't use, the associated metrics won't be exported.  ``PHAB_ID=D400382``

* finagle-zipkin-core: Tracing produces microsecond resolution timestamps in JDK9 or later.
  ``PHAB_ID=D400661``

* finagle-core: `Trace#time` and `Trace#timeFuture` no longer generate timestamped annotations or
  silently discard timing information. They now instead generate a `BinaryAnnotation` containing
  the timing information. In order to also get timestamped `Annotations` for when the operation
  began and ended, use in conjunction with `Trace#traceLocal`. ``PHAB_ID=D404869``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The `RetryPolicy` companion object is no longer a `JavaSingleton`.
  ``PHAB_ID=D399947``

* finagle-thrift: The RichClientParam constructors are now all either
  deprecated, so to construct it, you must call one of the RichClientParam.apply
  methods.  ``PHAB_ID=D400382``

Deprecations
~~~~~~~~~~~~

* finagle-core: Deprecate `Tracing#record(message, duration)` as it does not have the intended
  effect and silently discards any duration information in the resulting trace. Instead you should
  use either `Tracing#recordBinary` or a combination of `Trace#traceLocal` and `Trace#time`.
  ``PHAB_ID=D404869``

Bug Fixes
~~~~~~~~~

* finagle-core: `ClosableService` client stack module that prevents the reuse of closed services
  when `FactoryToService` is not set. This is important for clients making use of the `newClient`
  api. ``PHAB_ID=D407805``

19.11.0
-------

New Features
~~~~~~~~~~~~

* finagle-base-http: The `Uri` class now provides access publicly to its
  `path`, which is the request uri without the query parameters.
  ``PHAB_ID=D393893``

* finagle-mysql: Adding native support to finagle-mysql for MySQL JSON Data Type. A client
  can now  use `jsonAsObjectOrNull[T]` or `getJsonAsObject[T]` APIs on `c.t.f.mysql.Row` to
  read the underlying json value as type `T` or use `jsonBytesOrNull` API to get a raw byte
  array of the the json column value. ``PHAB_ID=D390914``

* finagle-mysql: MySQL integration tests can now run on a port other than the default (3306).
  Add a `port` property to `.finagle-mysql/integration-test.properties` to customize the value.
  ``PHAB_ID=D390914``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Upgrade to Netty 4.1.43.Final and netty-tcnative 2.0.26.Final. ``PHAB_ID=D389870``

* finagle: Add initial support for JDK 11 compatibility. ``PHAB_ID=D365075``

* finagle: Upgrade to caffeine 2.8.0 ``PHAB_ID=D384592``

* finagle-http2: Nacks in the form of RST(STREAM_REFUSED | ENHANCE_YOUR_CALM) no
  longer surface as a RstException, instead opting for a generic Failure to be
  symmetric with the HTTP/1.x nack behavior. ``PHAB_ID=D389234``

* finagle-mux: The mux handshake latency stat has be changed to Debug
  verbosity. ``PHAB_ID=D393158``

* finagle-serversets: `finagle/serverset2/stabilizer/notify_ms` histogram has been downgraded to
  debug verbosity. ``PHAB_ID=D392265``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: `c.t.f.http.codec.HttpContext` moved into `c.t.f.http.codec.context.HttpContext`
  ``PHAB_ID=D380407``

19.10.0
-------

New Features
~~~~~~~~~~~~

* finagle-partition: Enables cross-build for 2.13.0. ``PHAB_ID=D380444``

* finagle-exception: Enables cross-build for 2.13.0. ``PHAB_ID=D381107``

* finagle-exp: Enables cross-build for 2.13.0. ``PHAB_ID=D380497``

* finagle-http: Expose header validation API to public. ``PHAB_ID=D381771``

* finagle-mysql: Enables cross-build for 2.13.0. ``PHAB_ID=D377721``

* finagle-{mux,thrift,thrift-mux}: Enables cross-build for 2.13.0. ``PHAB_ID=D373165``

* finagle-netty4: Add support to stop default Finagle Netty 4 Timer. ``PHAB_ID=D381605``

* finagle-redis: Enables cross-build for 2.13.0. ``PHAB_ID=D381107``

* finagle-tunable: Enables cross-build for 2.13.0. ``PHAB_ID=D373170``

* finagle-grpc-context: Enables cross-build for 2.13.0. ``PHAB_ID=D373168``

* finagle-thrift: Pass a factory to create a TReusableBuffer as the parameter of a finagle client
  to allow multiple clients share one TReusableBuffer. ``PHAB_ID=D378466``

* finagle-zipkin-{core,scribe}: Enables cross-build for 2.13.0. ``PHAB_ID=D381675``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: Better performance for the default `HeaderMap.add` method for headers with
  the same name. ``PHAB_ID=D381142``

* finagle-http2: H2ServerFilter will no longer swallow exceptions that fire via
  `exceptionCaught` in the Netty pipeline. `PHAB_ID=D369185`

* finagle-http: Remove legacy HTTP/2 client implementation and make the `MultiplexHandler`-based
  implementation the default HTTP/2. ``PHAB_ID=D362950``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.l.FailureAccrualFactory`'s `didMarkDead()` changed to `didMarkDead(Duration)`.
  The `Duration` is the length of time the endpoint is marked dead. ``PHAB_ID=D369209``

* finagle-core: removed the `staticDetermisticApertureWidth` flag. The behavior is now as if the flag
  was set to `true` which was also the default behavior. ``PHAB_ID=D382779``

Bug Fixes
~~~~~~~~~

* finagle-mux: Mux now properly propagates `Ignorable` failures multiple levels for superseded
  backup requests. This allows for more accurate success rate metrics for downstream services,
  when using backup requests.
  ``PHAB_ID=D365729``

19.9.0
------

New Features
~~~~~~~~~~~~

* finagle-{core,init,toggle,netty4}: Enables cross-build for 2.13.0. ``PHAB_ID=D364013``

* finagle-base-http: Add `None` as a valid SameSite header value. ``PHAB_ID=D365170``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The constructor on `c.t.f.filter.NackAdmissionFilter` used for testing that
  took an `Ema.Monotime` has been removed. ``PHAB_ID=D351249``

* finagle-core: The `Adddress.ServiceFactory` variant has been promoted from experimental
  status and moved to be properly part of `c.t.f.Address`. ``PHAB_ID=D357122``

* finagle-http: improve performance of c.t.f.http.filter.StatsFilter. This results in two notable
  API changes:
    1. There is a `private[filter]` constructor which can take a `() => Long` for
       determining the current time in milliseconds (the existing `StatsFilter(StatsReceiver)`
       constructor defaults to using `Stopwatch.systemMillis` for determining the current time in
       milliseconds.
    2. The `protected count(Duration, Response)` method has been changed to
       `private[this] count(Long, Response)` and is no longer part of the public API.
  ``PHAB_ID=D350733``

* finagle-partitioning: the hash-based routing that memcached uses has been relocated to a new
  top-level module so that it can be used more broadly across protocols. This results
  in several classes moving to the c.t.f.partitioning package:
    1. The `Memcached.param.EjectFailedHost`, `KeyHasher`, and `NumReps` parameters are now
       available under `c.t.f.partitioning.param`
    2. The `FailureAccrualException` and `CacheNode` definitions are now in the `c.t.f.paritioning`
       package.
    3. The `ZkMetadata` class has moved to `c.t.f.p.zk` and the finagle-serverset module now depends
       on finagle-partitioning.
  ``PHAB_ID=D359303``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: `c.t.f.http.service.NotFoundService` has been changed to no longer
  use `Request.response`. Use of `Request.response` is deprecated and discouraged.
  ``PHAB_ID=D357348``

* finagle-mysql: Handshaking for the MySQL 'Connection Phase' now occurs as part of session
  acquisition. As part of this change, the
  `com.twitter.finagle.mysql.IncludeHandshakeInServiceAcquisition` toggle
  has been removed and it no longer applies. ``PHAB_ID=D355549``

* finagle: Upgrade to Netty 4.1.39.Final. ``PHAB_ID=D355848``

* finagle-http: Enable Ping Failure Detection for MultiplexHandler based HTTP/2 clients. Note that
  the Ping Failure Detection implementation has been removed completely from the
  non-MultiplexHandler based HTTP/2 client. ``PHAB_ID=D360712``

* finagle: Added a dependency on Scala Collections Compat 2.1.2. ``PHAB_ID=D364013``

Bug Fixes
~~~~~~~~~

* finagle-base-http: Removes the `Cookie` header of a `c.t.f.http.Message` whenever its cookie map
  becomes empty. ``PHAB_ID=D361326``

19.8.0
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The contents of the `c.t.f.dispatch.GenSerialClientDispatcher` object have been
  moved to the new `c.t.f.dispatch.ClientDispatcher` object. The stats receiver free constructors
  of `GenSerialClientDispatcher` and `SerialClientDispatcher` have been removed.
  ``PHAB_ID=D342883``

* finagle-thrift: The deprecated `ReqRepThriftServiceBuilder` object has been
  removed. Users should migrate to `ReqRepMethodPerEndpointBuilder`. ``PHAB_ID=D345740``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Failed reads on Linux due to a remote peer disconnecting should now be properly
  seen as `c.t.f.ChannelClosedException` instead of a `c.t.f.UnknownChannelException`.
  ``PHAB_ID=D336428``

* finagle: Upgrade to Jackson 2.9.9. ``PHAB_ID=D345969``

* finagle: Upgrade to Netty 4.1.38.Final. ``PHAB_ID=D346259``

* finagle-base-http: Moved c.t.f.http.serverErrorsAsFailures out of its package
  object, which changes its name from
  `com.twitter.finagle.http.package$serverErrorsAsFailures` to
  `com.twitter.finagle.http.serverErrorsAsFailures`. ``PHAB_ID=D353045``

* finagle-thrift: Moved c.t.f.thrift.maxReusableBufferSize out of its package
  object, which changes its name from
  `com.twitter.finagle.thrift.package$maxReusableBufferSize` to
  `com.twitter.finagle.thrift.maxReusableBufferSize`. ``PHAB_ID=D353045``

19.7.0
------

New Features
~~~~~~~~~~~~

* finagle-http: Measure streaming (message.isChunked) chunk payload size with two new histograms:
  `stream/request/chunk_payload_bytes` and `stream/response/chunk_payload_bytes`, they are
  published with a debug verbosity level. These chunk payload sizes are also traced via the same
  trace keys. ``PHAB_ID=D337877``

* finagle-base-http: Add support for new "b3" tracing header. ``PHAB_ID=D334419``

* finagle-core: Allow to not bypass SOCKS proxy for localhost by using the GlobalFlag
  `-com.twitter.finagle.socks.socksProxyForLocalhost` ``PHAB_ID=D337073``

* finagle-core: OffloadFilter flag to reduce network contention. ``PHAB_ID=D331502``

* finagle-exp: Add private `c.t.f.exp.ConcurrencyLimitFilter` for rejecting requests
  that exceed estimated concurrency limit ``PHAB_ID=D328815``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: `c.t.f.http.Cors` has been changed to no longer use the `c.t.f.http.Response`
  associated with the passed in `c.t.f.http.Request`. ``PHAB_ID=D332765``

* finagle-http: `c.t.f.http.filter.ExceptionFilter` has been changed to no longer
  use the `c.t.f.http.Response` associated with the passed in `c.t.f.http.Request`.
  ``PHAB_ID=D333509``

* finagle-http: Optimize creation of new Http Dispatchers by re-using created metrics and loggers.
  ``PHAB_ID=D335114``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: Removed the methods `setStatusCode` and `getStatusCode` from
  `c.t.f.http.Response` which have been deprecated since 2017. ``PHAB_ID=D326326``

* finagle-core: All deprecated `c.t.f.builder.ServerBuilder#build` methods have
  been removed. Users should migrate to using the `build` method which takes a
  `ServiceFactory[Req, Rep]` as a parameter. ``PHAB_ID=D331011``

* finagle-core: The `c.t.f.ssl.client.SslClientEngineFactory#getHostname` method has been removed.
  All uses should be changed to use the `getHostString` method of `SslClientEngineFactory` instead.
  ``PHAB_ID=DD334087``

* finagle-http: The `setOriginAndCredentials`, `setMaxAge`, `setMethod`, and `setHeaders` methods
  of `c.t.f.http.Cors.HttpFilter` are no longer overridable. ``PHAB_ID=D332765``

* finagle-http: The details of the `c.t.f.Http.HttpImpl` class are meant to be implementation
  details so the class constructor was made private along with the fields. Along these same lines
  the `c.t.f.Http.H2ClientImpl.transporter` method has been moved to a private location.
  ``PHAB_ID=D337136``

Bug Fixes
~~~~~~~~~

* finagle-core: Ensure ClientDispatcher `queueSize` gauge is removed on transport
  close, instead of waiting for clean-up at GC time. ``PHAB_ID=D331923``

* finagle-http2: Don't propagate stream dependency information for the H2 client.
  ``PHAB_ID=D332191``

19.6.0
------

New Features
~~~~~~~~~~~~

* finagle-core: SSL/TLS session information has been added to `c.t.f.ClientConnection`.
  ``PHAB_ID=D323305``

* finagle-core: Add a Stack Module with 7 parameters for convenience sake. ``PHAB_ID=D325382``

* finagle-core: For both, servers and clients, introduce a way to shift application-level future
  callbacks off of IO threads, into a given `FuturePool` or `ExecutorService`.
  Use `withExecutionOffloaded` configuration method (on a client or a server) to access
  new functionality. ``PHAB_ID=D325049``

* finagle-http: Added counters for request/response stream as: `stream/request/closed`,
  `stream/request/failures`, `stream/request/failures/<exception_name>`, `stream/request/opened`,
  `stream/request/pending` and `stream/response/closed`, `stream/response/failures`,
  `stream/response/failures/<exception_name>`, `stream/response/opened`, `stream/response/pending`.
  The counters will be populated when `isChunked` is set to true, the failures counters will be
  populated when `isChunked` is set to true and the stream fails before it has been fully read in the
  request and response respectively.  ``PHAB_ID=D315041``

* finagle-http: Add two new API variants in `CookieMap`: `addAll` and `removeAll` that allow for
  adding and removing cookies in bulk, without triggering a header rewrite on each item.
  ``PHAB_ID=D318013``

* finagle-mysql: finagle-mysql now supports using SSL/TLS with MySQL. SSL/TLS can be turned on by
  calling `withTransport.tls(sslClientConfiguration)` with a specified
  `c.t.f.ssl.client.SslClientConfiguration`. ``PHAB_ID=D328077``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Upgrade to Netty 4.1.35.Final and netty-tcnative 2.0.25.Final.
  ``PHAB_ID=D312439``

* finagle-core: The default failure accrual policy has been changed from one
  which uses only consecutive failures to a hybrid model which uses both
  success rate over a window and consecutive failures. Previously this was
  changeable via toggle. The toggle has been removed, and the hybrid version
  has been made the default. ``PHAB_ID=D327394``

* finagle-http: Rename `request_stream_duration_ms` to `stream/request/duration_ms` and
  `response_stream_duration_ms` to `stream/response/duration_ms`. The stats will be
  populated when `isChunked` is set to true in the request and response respectively.
  ``PHAB_ID=D315041``

* finagle-http2: Disable ping-based failure detector in HTTP/2 client as it seems to do
  more harm than good.  ``PHAB_ID=D322357``

* finagle-http2: Frame logging is now disabled by default for clients. To enable,
  use the `c.t.f.http2.param.FrameLogging.Enabled` Stack Param. For example:
  `Http.client.configured(FrameLogging.Enabled)`. ``PHAB_ID=D326727``

* finagle-netty4: When using a Netty `LocalChannel`, the value of the `BackPressure`
  stack param is effectively changed to `backPressureDisabled` so that other functionality
  (e.g. SSL/TLS) works as expected. ``PHAB_ID=D319011``

* finagle-netty4: `finagle/netty/pooling/used` now includes the size of the buffers in the
  thread-local caches.  ``PHAB_ID=D320021``

* finagle-core: Stats and retry modules use a ResponseClassifier to give hints
  for how to handle failure (e.g., Is this a success or is it a failure? If
  it's a failure, may I retry the request?). The stats module increments a
  success counter for successes, and increments a failure counter for failures.
  But there isn't a way to tell the stats module to just do nothing. And, this
  is exactly what the stats module should do (nothing) in the case of ignorable
  failures (e.g. backup request cancellations). To represent these cases, we
  introduce a new ResponseClass: Ignorable. ``PHAB_ID=D316884``

Bug Fixes
~~~~~~~~~

* finagle-core: `UsingSslSessionInfo` would fail to be constructed properly when
  `SSLSession.getLocalCertificates` returns 'null'. ``PHAB_ID=D324499``

* finagle-http: Finagle now properly sets the `Transport.peerCertificate` local context
  when using HTTP/2. ``PHAB_ID=D324392``

* finagle-http: `c.t.f.http.collection.RecordSchema.Record` is now thread-safe.
  ``PHAB_ID=D325700``

* finagle-zipkin-core: Fix a race condition which could cause a span to get logged
  missing some annotations. ``PHAB_ID=D319367``

* finagle-mysql: Don't log `c.t.f.ChannelClosedException` when rolling back a transaction
  fails. ``PHAB_ID=D327111``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The exceptions `c.t.f.SslHandshakeException` and
  `c.t.f.SslHostVerificationException` were no longer used and have
  been removed. ``PHAB_ID=D330138``

* finagle-mysql: The structure of `c.t.f.mysql.Request` has changed. It is now based on
  a higher level `c.t.f.mysql.ProtocolMessage` and the `cmd` field must contain a value.
  Additionally, the synthetic `Command.COM_NO_OP` has been removed, as due to the
  restructuring it was no longer necessary. ``PHAB_ID=D327554``

* finagle-mysql: Uses of the abbreivation 'cap' have been renamed to the full
  word: 'capabilities', including for the `baseCapabilities` of `Capability`.
  ``PHAB_ID=D329603``

Deprecations
~~~~~~~~~~~~

* finagle-http: Removed deprecated `response_size` in Finagle Http stats. This is a duplicate stat
  of `response_payload_bytes`. PHAB_ID=D328254``

19.5.1
------

No Changes

19.5.0
------

New Features
~~~~~~~~~~~~

* finagle-http: Add two new methods to `com.twitter.finagle.http.MediaType`,
  `MediaType#typeEquals` for checking if two media types have the same type and
  subtype, ignoring their charset, and `MediaType#addUtf8Charset` for easily
  setting a utf-8 charset.  ``PHAB_ID=D308761``

Bug Fixes
~~~~~~~~~

* finagle-http: Ensure server returns 400 Bad Request when
  non-ASCII characters are present in the HTTP request URI path. ``PHAB_ID=D312009``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Deterministic aperture (d-aperture) load balancers no longer export
  "loadband" scoped metrics: "widen", "narrow", "offered_load_ema". These were not
  necessary as d-aperture does not change the aperture size at runtime. ``PHAB_ID=D303833``

* finagle-core: Request logging now defaults to disabled. Enable it by configuring the
  `RequestLogger` Stack parameter on your `Client` or `Server`. ``PHAB_ID=D308476``

* finagle-core: Subtree binding failures in `NameTree.Union`'s are ignored in the
  final binding result. ``PHAB_ID=D315282``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The `c.t.f.client.EndpointerModule` and `c.t.f.pushsession.PushStackClient` public
  and protected APIs have been changed to use the abstract `java.net.SocketAddress` instead of the
  concrete `java.net.InetSocketAddress` as relying on the concrete implementation was not
  necessary. ``PHAB_ID=D315111``

* finagle-http: For Finagle HTTP clients, the `withMaxRequestSize(size)` API
  method has been removed. For Finagle HTTP servers, the
  `withMaxResponseSize(size)` method has been removed. The underlying `Stack`
  params which are set by these methods are respectively HTTP server and HTTP
  client side params only. Using these removed methods had no effect on the
  setup of Finagle HTTP clients and servers. ``PHAB_ID=D314019``

* finagle-mysql: HandshakeResponse has been removed from finagle-mysql's public
  API. It is expected that users of the library are relying entirely on
  finagle-mysql for handshaking. ``PHAB_ID=D304512``

19.4.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Make maxDepth in Namer configurable. ``PHAB_ID=D286444``

  - namerMaxDepth in Namer now configurable through a global flag (namerMaxDepth)

* finagle-core: The newly renamed `SslSessionInfo` is now public. It is
  intended for providing information about a connection's SSL/TLS session.
  ``PHAB_ID=D286242``

* finagle-core: Added the `c.t.finagle.DtabFlags` trait which defines a Flag and function for
  appending to the "base" `c.t.finagle.Dtab` delegation table. ``PHAB_ID=D297596``

* finagle-http: Finagle HTTP implementation now supports trailing headers (trailers). Use
  `c.t.f.http.Message.trailers` to access trailing headers on a fully-buffered message
  (`isChunked == false`) or `c.t.f.http.Message.chunkReader` on a message with chunked payload
  (`isChunked == true`).  ``PHAB_ID=D283999``

* finagle-http,thriftmux: Added tracing annotations to backup requests. ``PHAB_ID=D285486``

  - Binary annotation "srv/backup_request_processing", when servers are processing backup requests.

* finagle-http: Added new server metrics to keep track of inbound requests that are rejected due to
  their headers containing invalid characters (as seen by RFC-7230): `rejected_invalid_header_names`
  and `rejected_invalid_header_values`. ``PHAB_ID=D294754``

* finagle-http: Added stats of the duration in milliseconds of request/response streams:
  `request_stream_duration_ms` and `response_stream_duration_ms`. They are enabled by using
  `.withHttpStats` on `Http.Client` and `Http.Server`  ``PHAB_ID=D297900``

* finagle-mysql: A new toggle, "com.twitter.finagle.mysql.IncludeHandshakeInServiceAcquisition", has
  been added. Turning on this toggle will move MySQL session establishment (connection phase) to be
  part of service acqusition. ``PHAB_ID=D301456``

* finagle-core: Support for MethodBuilder and stack construction outside of `c.t.f` package.
  ``PHAB_ID=D275053``.
  This includes:
  - `c.t.f.client.MethodBuilder` is now public.
  - construction of the following stack modules are now public: `c.t.f.factory.TimeoutFactory`,
    `c.t.f.filter.ExceptionSourceFilter`, `c.t.f.loadbalancer.LoadBalancerFactory`,
    `c.t.f.service.Retries`

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Client-side nacking admission control now defaults on. See the documentation
  on `c.t.f.filter.NackAdmissionFilter` for details. This can be disabled by setting the
  global flag, `com.twitter.finagle.client.useNackAdmissionFilter`, to false.
  ``PHAB_ID=D289583``

* finagle-core: `LatencyCompensation` now applies to service acquisition. ``PHAB_ID=D285574``

* finagle-http: HTTP headers validation on the outbound path is now in compliance with RFC7230.
  ``PHAB_ID=D247125``

* finagle-netty4: Netty's reference leak tracking now defaults to disabled.
  Set the flag `com.twitter.finagle.netty4.trackReferenceLeaks` to `true` to enable.
  ``PHAB_ID=D297031``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: Dropped a dependency on Netty 3:
 - finagle-netty3 sub-project has been removed
 - finagle-http-cookie sub-project has been removed
 - `c.t.f.http.Cookie` no longer takes Netty's `DefaultCookie` in the constructor
 ``PHAB_ID=D291221``


* finagle-core: The `peerCertificate` methods of `c.t.f.t.TransportContext` and
  `c.t.f.p.PushChannelHandle` have been replaced with the more robust
  `sslSessionInfo`. Users looking for just the functional equivalence of
  `peerCertificate` can use `sslSessionInfo.peerCertificates.headOption`.
  ``PHAB_ID=D285926``

* finagle-core: The `com.twitter.finagle.core.UseClientNackAdmissionFilter` toggle
  has been replaced by a global flag, `com.twitter.finagle.client.useNackAdmissionFilter`.
  ``PHAB_ID=D289583``

* finagle-thrift: Allow users to specify stringLengthLimit and containerLengthLimit ``PHAB_ID=D286346``
  - method parameter `readLength` in com.twitter.finagle.thrift.Protocols#binaryFactory renamed to stringLengthLimit to reflect usage
  - method parameter `containerLengthLimit` added to com.twitter.finagle.thrift.Protocols#binaryFactory

19.3.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Added tracing annotations to backup requests. ``PHAB_ID=D280998``

  - Timestamped annotation "Client Backup Request Issued"
  - Timestamped annotation "Client Backup Request Won" or "Client Backup Request Lost"
  - Binary annotation "clnt/backup_request_threshold_ms", with the current value of the latency threshold, in milliseconds
  - Binary annotation "clnt/backup_request_span_id", with the span id of the backup request

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Deprecated multi-param legacy `tls` methods have been removed in
  `c.t.f.param.ServerTransportParams` and `c.t.f.builder.ServerBuilder`. Users should migrate
  to using the `tls(SslServerConfiguration)` method instead. ``PHAB_ID=D277045``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: The tracing annotations from `MkJvmFilter` have been enhanced. ``PHAB_ID=D282590``

  - Timestamped annotations "GC Start" and "GC End" for each garbage collection
    event that occurred during the request.
  - Binary annotation "jvm/gc_count", with the total number of garbage collection
    events that occurred during the request.
  - Binary annotation "jvm/gc_ms", with the total milliseconds of garbage collection
    events that occurred during the request.

19.2.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Added gauge `is_marked_dead` as an indicator of whether the host is marked
  as dead(1) or not(0) in `FailFastFactory`. ``PHAB_ID=D263552``

* finagle-core: `KeyCredentials.CertsAndKey` has been added as an option for
  `c.t.f.ssl.KeyCredentials` for when the certificate and certificate chain are
  contained within the same file. ``PHAB_ID=D264325``

* finagle-thriftmux: Additional information is now annotated in traces for clients
  using Scrooge generated Thrift bindings. ``PHAB_ID=D269383``, ``PHAB_ID=D270597``,
  ``PHAB_ID=D272934``.
  This includes:

  - RPC method name
  - Request serialization time, in nanoseconds
  - Request deserialization time, in nanoseconds
  - Response serialization time, in nanoseconds
  - Response deserialization time, in nanoseconds


Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-http: Removed `Http.Client.withCompressionLevel` because it wasn't doing anything.
  To migrate your client, simply remove the configuration--it had absolutely no effect.
  ``PHAB_ID=D260077``

* finagle-http: `c.t.f.dispatch.ExpiringServerDispatcher` was dead code. We removed it.
  ``PHAB_ID=D269331``

* finagle-thrift: Removed `newIface` and `newServiceIface` methods from
  `c.t.f.thrift.ThriftRichClient.MultiplexedThriftClient`, which are deprecated in November 2017.
  ``PHAB_ID=D271774``

* finagle-thrift: Removed deprecated APIs located in Thrift.scala: ``PHAB_ID=D272811``

    1. c.t.f.Thrift.Client.stats => use c.t.f.Thrift.Client.clientParam.clientStats
    2. c.t.f.Thrift.withProtocolFactory => use c.t.f.Thrift.client.withProtocolFactory
    3. c.t.f.Thrift.withClientId => use c.t.f.Thrift.client.withClientId
    4. c.t.f.Thrift.Server.serverLabel => use c.t.f.Thrift.Server.serverParam.serviceName
    5. c.t.f.Thrift.Server.serverStats => use c.t.f.Thrift.Server.serverParam.serverStats
    6. c.t.f.Thrift.Server.maxThriftBufferSize => use c.t.f.Thrift.Server.serverParam.maxThriftBufferSize

* finagle-thrift: `c.t.f.thrift.ThriftServiceIface.Filterable` is removed, use
  `c.t.f.thrift.service.Filterable` instead. ``PHAB_ID=D272427``

* finagle-thrift: `c.t.f.thrift.ThriftServiceIface` is removed, use
  `c.t.f.thrift.service.ThriftServicePerEndpoint` instead. ``PHAB_ID=D272427``

* finagle-thriftmux: Removed deprecated APIs located in ThriftMux.scala: ``PHAB_ID=D272811``

    1. c.t.f.ThriftMux.Client.stats => use c.t.f.ThriftMux.Clien.clientParam.clientStats
    2. c.t.f.ThriftMux.Server.serverLabel => use c.t.f.ThriftMux.Server.serverParam.serviceName
    3. c.t.f.ThriftMux.Server.serverStats => use c.t.f.ThriftMux.Server.serverParam.serverStats
    4. c.t.f.ThriftMux.Server.maxThriftBufferSize => use c.t.f.ThriftMux.Server.serverParam.maxThriftBufferSize

* finagle-thriftmux: `ThriftMux.Client.pushMuxer` is removed. Use `ThriftMux.Client.standardMuxer`
  instead. ``PHAB_ID=D269373``

* finagle-thriftmux: `ThriftMux.serverMuxer` is removed. Use `ThriftMux.Server.defaultMuxer`
  instead. ``PHAB_ID=D269373``

* finagle-base-http: Removed the `c.t.f.http.Statuses` java helper, which was deprecated two years
  ago in favor of using `c.t.f.http.Status` directly. ``PHAB_ID=D269224``

* finagle-base-http: Removed the `c.t.f.http.Versions` java helper, which was deprecated two years
  ago in favor of using `c.t.f.http.Version` directly. ``PHAB_ID=D269207``

* finagle-base-http: Removed the `c.t.f.http.Methods` java helper, which was deprecated two years
  ago in favor of using `c.t.f.http.Method` directly. ``PHAB_ID=D273235``

* finagle-http: `c.t.f.http.Response.Ok` was removed. Use just `Response()` or `Response.Proxy`
  if you need to mock it. ``PHAB_ID=D269737``

* finagle-core: `Drv.Aliased` and `Drv.newVose` are now private, please
  construct a `Drv` instance using `Drv.apply` or `Drv.fromWeights`.
  ``PHAB_ID=D262960``

* finagle-core: `c.t.f.BackupRequestLost` is now removed. Please use `c.t.f.Failure.ignorable`
  instead. ``PHAB_ID=D270833``

Bug Fixes
~~~~~~~~~

* finagle-http: Fix for a bug where HTTP/2 clients could retry requests that had a chunked
  body even if the request body was consumed. ``PHAB_ID=D258719``

* finagle-http: Fix for a bug where HTTP clients could assume connections are reusable, despite
  having streaming requests in flight. ``PHAB_ID=D264985``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Faster `Filters`. Removes unnecessary `Service.rescue` proxies from
  the intermediate `andThen`-ed `Filters`. Previously in rare cases you might have seen
  a raw `Exception` not wrapped in a `Future` if the `Filter` threw. These will now
  consistently be lifted into a `Future.exception`. ``PHAB_ID=D269003``

* finagle-core: MethodBuilder metrics filtering updated to now report rolled-up
  logical failures. ``PHAB_ID=D271195``

* finagle-http: Disabling Netty3 cookies in favor of Netty4 cookies. ``PHAB_ID=D262776``

* finagle-http: Removed the debug metrics `http/cookie/dropped_samesites` and
  `http/cookie/flagless_samesites`. ``PHAB_ID=D267239``

Deprecations
~~~~~~~~~~~~

* finagle-core: Multi-param legacy `tls` methods have been deprecated in
  `c.t.f.param.ServerTransportParams` and `c.t.f.builder.ServerBuilder`. Users should migrate
  to using the `tls(SslServerConfiguration)` method instead. ``PHAB_ID=D265844``

* finagle-core: `$client.withSession.maxIdleTime` is now deprecated; use
  `$client.withSessionPool.ttl` instead to set the maximum allowed duration a connection may be
  cached for.  ``PHAB_ID=D272370``

* finagle-serversets: `c.t.f.zookeeper.ZkResolver` has been deprecated in favor
  of `c.t.f.serverset2.Zk2Resolver`. ``PHAB_ID=D273608``

19.1.0
-------

New Features
~~~~~~~~~~~~

* finagle-core: `c.t.f.s.StackBasedServer` has been changed to extend the
  `c.t.f.Stack.Transformable` trait. This brings `StackBasedServer` into parity
  with `c.t.f.c.StackBasedClient`, which already extends the
  `Stack.Transformable` trait. ``PHAB_ID=D253542``

* finagle-http: HttpMuxer propagates the close signal to the underlying handlers.
  ``PHAB_ID=D254656``

* finagle-stats-core: introduce flag to allow logging metrics on service shutdown.
  ``PHAB_ID=D253590``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The deprecated `c.t.f.b.ServerBuilder.stack` method which takes a function
  has been removed. Uses of this method should be changed to use the `c.t.f.b.ServerBuilder.stack`
  method which takes a `c.t.f.s.StackBasedServer` instead. ``PHAB_ID=D251975``

* finagle-core: The type of `c.t.f.b.ServerConfig.nilServer` has been changed from
  `Server[Req, Rep]` to `StackBasedServer[Req, Rep]`. ``PHAB_ID=D252142``

* finagle-core: The access level of the `c.t.f.b.ServerBuilder.copy` method has changed
  from protected to private. ``PHAB_ID=D252142``

* finagle-core: The bridge type `c.t.f.b.Server` has been removed. Users should
  change to use `c.t.f.ListeningServer` instead. Uses of the previously
  deprecated `Server.localAddress` should use `ListeningServer.boundAddress`
  instead. ``PHAB_ID=D254339``

* finagle-core: The deprecated `c.t.f.t.Transport.localAddress` and
  `c.t.f.t.Transport.remoteAddress` methods are now final and can no longer
  be extended. Users should migrate to the respective `c.t.f.t.TransportContext`
  methods. ``PHAB_ID=D256257``

* finagle-thrift: The `c.t.f.t.ThriftRichClient.protocolFactory` and
  `c.t.f.t.ThriftRichServer.protocolFactory` methods have been removed. Users should
  switch to using `ThriftRichClient.clientParam.protocolFactory` and
  `ThriftRichServer.serverParam.protocolFactory` instead. In addition, implementations
  of the `protocolFactory` method have been removed from the concrete `c.t.f.Thrift`
  and `c.t.f.ThriftMux` client and server. ``PHAB_ID=D256217``

Bug Fixes
~~~~~~~~~

* finagle-core: Failed writes on Linux due to a remote peer disconnecting should now
  be properly seen as a `c.t.f.ChannelClosedException` instead of a
  `c.t.f.UnknownChannelException`. ``PHAB_ID=D256007``

* finagle-http: Compression level of 0 was failing on the server-side when speaking h2c.
  Updated so that it can handle a request properly. ``PHAB_ID=D251320``

* finagle-thriftmux: A Java compatibility issue for users trying to call `withOpportunisticTls`
  on `ThriftMux` clients and servers has been fixed. ``PHAB_ID=D256027``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `ServiceFactory.const` propagates the close from the `ServiceFactory`
  to the underlying service, instead of ignoring it. ``PHAB_ID=D254656``

18.12.0
-------

New Features
~~~~~~~~~~~~

* finagle-redis: Add support for the new stream API released in Redis 5.0. ``PHAB_ID=D244329``

* finagle-core: Add Java compatibility for `c.t.f.Filter.TypeAgnostic.Identity`
  via `c.t.f.Filter.typeAgnosticIdentity()`. ``PHAB_ID=D242006``

* finagle-core: Add Java compatibility for `c.t.f.Name` through `Names`.
  ``PHAB_ID=D242084``

* finagle-core: Introduce a `StackServer.withStack` overload that
  makes modifying the existing `Stack` easier when using method chaining.
  ``PHAB_ID=D246893``

* finagle-stats: Split the implementation and `ServiceLoading` into separate modules.
  The implementation is in `finagle-stats-core`. This is backwards compatible
  for existing users of `finagle-stats` while allowing new usages built on top.
  ``PHAB_ID=D249875``

* finagle-thrift: Add `c.t.finagle.thrift.MethodMetadata` which provides a `LocalContext` Key
  for setting information about the current Thrift method and an accessor for retrieving
  the currently set value. ``PHAB_ID=D241295``

* finagle-thrift: Update `c.t.finagle.thrift.MethodMetadata` to provide an
  `asCurrent` method to set the current `c.t.finagle.thrift.MethodMetadata` in the
  `LocalContext`. ``PHAB_ID=D243625``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The `c.t.u.Closable` trait has been removed from
  `c.t.f.t.TransportContext`, as well as the `close` and `onclose` methods. Uses of
  these methods within `TransportContext` should be changed to use the corresponding
  methods on `c.t.f.t.Transport` instead. ``PHAB_ID=D244742``

* finagle-core: The deprecated `c.t.f.t.Transport.peerCertificate` method on the `Transport` class
  (not the `Transport.peerCertificate` Finagle context) has been removed. Uses of this
  method should be changed to use `c.t.f.t.TransportContext.peerCertificate` instead.
  ``PHAB_ID=D250027``

* finagle-core: The deprecated `c.t.f.t.TransportContext.status` method has been removed
  from `TransportContext`. Uses of this method should be changed to use
  `c.t.f.t.Transport.status` instead. ``PHAB_ID=D247234``

* finagle-mysql: `c.t.f.m.Charset` has been renamed to `c.t.f.m.MysqlCharset` to resolve
  any ambiguity between it and the `Charset` `Stack` parameter. ``PHAB_ID=D240965``

* finagle-mysql: All `Stack` params (`Charset`, `Credentials`, `Database`, `FoundRows`,
  `MaxConcurrentPrepareStatements`, `UnsignedColumns`) have been moved to the
  `com.twitter.finagle.mysql.param` namespace. ``PHAB_ID=D242473``

* finagle-mysql: The deprecated `c.t.f.m.Client.apply(factory, statsReceiver)` method
  has been removed. ``PHAB_ID=D243038``

* finagle-mysql: The `c.t.f.m.Handshake` class and companion object have been made
  private. ``PHAB_ID=D244734``

* finagle-http: Rename the toggle 'c.t.f.h.UseH2CClients' to 'c.t.f.h.UseH2CClients2'.
  ``PHAB_ID=D247320``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Upgrade to Netty 4.1.31.Final and netty-tcnative 2.0.19.Final. ``PHAB_ID=D235402``

* finagle-base-http: The `DefaultHeaderMap` will replace `obs-fold` ( CRLF 1*(SP/HTAB) ) in
  inserted header values. ``PHAB_ID=D245928``

* finagle-core: `MethodBuilder#idempotent` and `MethodBuilder#nonIdempotent` will no longer
  clobber `MethodBuilder.withRetries.withClassifier`. ``PHAB_ID=D255275``

18.11.0
-------

New Features
~~~~~~~~~~~~

* finagle-base-http: Add `Message.httpDateFormat(millis)` to format the epoch millis into
  an RFC 7231 formatted String representation. ``PHAB_ID=D234867``

* finagle-core: Introduce a `StackClient.withStack` overload that
  makes modifying the existing `Stack` easier when using method chaining.
  ``PHAB_ID=D234739``

* finagle-mysql: Introduce `session` to be able to perform multiple operations that require
  session state on a guaranteed single connection. ``PHAB_ID=D219322``

* finagle-netty4: When using the native epoll transport, finagle now publishes the TCP window size
  and number of retransmits based on the `tcpInfo` provided by from the channel.  These stats are
  published with a debug verbosity level.  ``PHAB_ID=D218772``

* finagle-http: HTTP clients and servers now accept `fixedLengthStreamedAfter` param in their
  `withStreaming` configuration (default: 5 MB when streaming is enabled). This new parameter
  controls the limit after which Finagle will stop aggregating messages with known `Content-Length`
  (payload will be available at `.content`) and switch into a streaming mode (payload will be
  available at `.reader`). Note messages with `Transfer-Encoding: chunked` never aggregated.
  ``PHAB_ID=D236573``

* finagle-thrift: `tracing.thrift` now has an optional timestamp field for a `Span`.
  ``PHAB_ID=D242204``

* finagle-zipkin-core: A Span now encodes a timestamp of when it was created as part
  of its thrift serialization. ``PHAB_ID=D242204``


Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-http: `c.t.f.http.param.MaxChunkSize` has been removed. There is no good reason to
  configure it with anything but `Int.MaxValue` (unlimited). ``PHAB_ID=D233538``

* finagle-exp: Update `DarkTrafficFilter#handleFailedInvocation` to accept the request type
  for more fidelity in handling the failure. ``PHAB_ID=D237484``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: Unset `maxChunkSize` limit in Netty HTTP codecs. Now both clients and servers
  emit all available data as a single chunk so we can put it into use quicker.
  ``PHAB_ID=D233538``

* finagle-http: Streaming clients (`withStreaming(true)`) now aggregate inbound messages with known
  `Content-Length` if their payloads are less than 5mb (8k before). Use `withStreaming(true, 32.kb)`
  to override it with a different value. ``PHAB_ID=D234882``

* finagle-http2: HTTP/2 servers perform a more graceful shutdown where an initial
  GOAWAY is sent with the maximum possible stream id and waits for either the client
  to hang up or for the close deadline, at which time a second GOAWAY is sent with
  the true last processed stream and the connection is then closed.
  ``PHAB_ID=D206683``

Deprecations
~~~~~~~~~~~~

* finagle-core: Deprecate
  `EndpointerStackClient.transformed(Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]])`
  in favor of the `withStack` variant. ``PHAB_ID=D234739``

18.10.0
-------

Deprecations
~~~~~~~~~~~~

* finagle-core: Deprecation warnings have been removed from the 'status', 'onClose',
  and 'close' methods on `c.t.f.t.Transport`, and added to the corresponding methods
  on `c.t.f.t.TransportContext`. ``PHAB_ID=D221528``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-netty3: Implementations for 'status', 'onClose', and 'close' methods have
  been moved from `c.t.f.n.t.ChannelTransportContext` to `c.t.f.n.t.ChannelTransport`.
  ``PHAB_ID=D221528``

18.9.1
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: `DefaultHeaderMap` now validates HTTP Header names and
  values in `add` and `set`. `addUnsafe` and `setUnsafe` have been created to
  allow adding and setting headers without validation. ``PHAB_ID=D217035``

* finagle-core: Remove slow host detection from `ThresholdFailureDetector`.
  ``PHAB_ID=D210015``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: When Finagle would exhaust a retry budget with an exception that was
  not a `FailureFlags`, previously it would wrap that exception with a non-retryable
  failure. This lead to surprising behavior for users. Those exceptions will no longer
  be wrapped. ``PHAB_ID=D216281``

* finagle-http: The finagle HTTP clients and servers now consider a `Retry-After: 0`
  header to be a retryable nack. Servers will set this header when the response is
  a retryable failure, and clients will interpret responses with this header as a
  `Failure.RetryableNackFailure`. ``PHAB_ID=D216539``

18.9.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `c.t.f.FailureFlags` is now a public API. This is Finagle's
  API for attaching metadata to an exception. As an example this is used to
  check if an exception is known to be safe to retry. Java compatibility has
  also been added. ``PHAB_ID=D202374``

* finagle-core: Introducing StackTransformer, a consistent mechanism for
  accessing and transforming the default ServerStack. ``PHAB_ID=D207980``

* finagle-netty4: Allow sockets to be configured with the [SO_REUSEPORT](https://lwn.net/Articles/542629/) option
  when using native epoll, which allows multiple processes to bind and accept connections
  from the same port. ``PHAB_ID=D205535``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: `c.t.io.Reader` and `c.t.io.Writer` are now abstracted over the type
  they produce/consume (`Reader[A]` and `Writer[A]`) and are no longer fixed to `Buf`.
  ``PHAB_ID=D195638``

* finagle-core: `Address.hashOrdering` now takes a seed parameter and
  `PeerCoordinate.setCoordinate` does not take a `peerOffset` any longer.
  ``PHAB_ID=D199545``

* finagle-core: Removed deprecated members `c.t.f.Failure.{Interrupted, Ignorable, DeadlineExceeded,
  Rejected, NonRetryable, flagsOf}`. ``PHAB_ID=D199361``

* finagle-core: SingletonPool now takes an additional parameter which indicates if interrupts
  should propagate to the underlying resource. ``PHAB_ID=D205433``

* finagle-core: Remove `TimeoutFactory.Role` in favor of passing a role to the `module` function.
  Since this module is a re-used within the client stack, it needs unique identifiers for each
  distinct module. ``PHAB_ID=D204647``

* finagle-core: the valid range for the argument to `WindowedPercentileHistogram.percentile`
  is now [0.0..1.0], e.g., 0.95 means 95th percentile. ``PHAB_ID=D198915``

* finagle-mux: The old pull-based mux implementations have been removed. ``PHAB_ID=D208737``

* finagle-netty3: The type of context of a `ChannelTransport` has been changed from a
  `LegacyContext` to a `ChannelTransportContext`. ``PHAB_ID=D205473``

* finagle-netty4: The type of context of a `ChannelTransport` has been changed from a
  `Netty4Context` to a `ChannelTransportContext`. ``PHAB_ID=D205794``

* finagle-netty4: `c.t.f.netty4.param.useUnpoolledByteBufAllocator` flag has been removed. There is
  no good reason to opt-out of a more efficient, pooled allocator. ``PHAB_ID=D212097``

* finagle-thrift: `DeserializeCtx` became `ClientDeserializeCtx` for client side response
  classification, add `ServerDeserializeCtx` to handle server side response classification.
  ``PHAB_ID=D196032``

* finagle-serversets: `ZkMetadata.shardHashOrdering` now takes a seed parameter.
  ``PHAB_ID=D199545``

Bug Fixes
~~~~~~~~~

* finagle-thrift: Thrift clients created via `.servicePerEndpoint` now propagate exceptions
  appropriately when the method return type is void. ``PHAB_ID=D200690``

* finagle-thrift, finagle-thriftmux: Response classification is enabled in server side.
  ``PHAB_ID=D196032``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-memcached: A Memcached client (`c.t.f.Memcached.Client`) is now backed by a more efficient,
  push-based implementation. ``PHAB_ID=D208047``

* finagle-netty4: Finagle's Netty 4 implementation now defaults to use Linux's native epoll
  transport, when available. Run with `-com.twitter.finagle.netty4.useNativeEpoll=false` to opt out.
  ``PHAB_ID=D208088``

18.8.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Introducing the new `c.t.f.tracing.Tracing` API for more efficient tracing
  (dramatically reduces the number of context lookups; see scaladoc for `c.t.f.tracing.Trace`).
  ``PHAB_ID=D190670``

* finagle-core: `c.t.f.tracing.Trace` facade API now provides forwarding `record` methods for
  all kinds of annotations and is a preffered way of recording traces. ``PHAB_ID=D192598``

* finagle-thriftmux: Promote the push-based ThriftMux implementation out of experimental
  status.``PHAB_ID=D189187``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: `c.t.f.http.cookie.exp.supportSameSiteCodec` has been moved out of the
  exp package to `c.t.f.http.cookie.supportSameSiteCodec`. ``PHAB_ID=D196517``

* finagle-core: Parameter-less annotation classes (`c.t.f.tracing.Annotation`) have been
  promoted to objects for efficiency reasons.  ``PHAB_ID=D192598``

* finagle-core: `c.t.f.tracing.Trace.record(Record)` now accepts the record argument by
  value (previously by name). ``PHAB_ID=D193300``

* finagle-core: `c.t.f.Failure.{Restartable, Interrupted, Ignorable, DeadlineExceeded,
  Wrapped, Rejected, NonRetryable}` are deprecated in favor of the `c.t.f.FailureFlags`
  analogs. ``PHAB_ID=D195647``

* finagle-core: `c.t.f.Leaf` and `c.t.f.Node` are now private; use `Stack.leaf` and
  `Stack.node` instead. ``PHAB_ID=D195924``

* finagle-core: Marked `transform` in `com.twitter.finagle.Stack` as protected. It is too
  powerful and unnecessary for users, and should be for implementor use only. ``PHAB_ID=D195938``

* finagle-mysql: `c.t.f.mysql.CanBeParameter`'s implicit conversions `timestampCanBeParameter`,
  `sqlDateCanBeParameter`, and `javaDateCanBeParameter` have been consolidated into a single
  implicit, `dateCanBeParameter`. ``PHAB_ID=D195351``

Bug Fixes
~~~~~~~~~

* finagle-http2: Fixed a race condition caused by c.t.f.http.transport.StreamTransports being
  closed, but that status not being reflected right away, causing a second request to fail.
  ``PHAB_ID=D198198``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.tracing.Trace` API is no longer guarding `Trace.record` calls behind
  `Trace.isActivelyTracing`. Add `Trace.isActivelyTracing` guards on the call sites if
  materializing tracing annotations is a performance concern.  ``PHAB_ID=D193300``

* finagle-mysql: Clients will now issue a ROLLBACK each time a service is checked back
  into the connection pool. This can be disabled via `Mysql.Client.withNoRollback`.
  ``PHAB_ID=D196673``

* finagle-thriftmux: The push-based server muxer is now the default. In both synthetic tests
  and production it has shown signifcant performance benefits and is simpler to maintain.
  ``PHAB_ID=D193630``

* finagle-mysql: Remove deprecated `TimestampValue.apply(Timestamp)` and
  `TimestampValue.unapply(value)` methods. Use `TimestampValue.apply(TimeZone, TimeZone)`
  instead. ``PHAB_ID=D182920``

Deprecations
~~~~~~~~~~~~

* finagle-mux: The pull based mux implementation, c.t.f.Mux, has been deprecated in favor of
  the push-based mux implementation, c.t.f.pushsession.MuxPush. ``PHAB_ID=D193630``

18.7.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Promote the push-based API's out of experimental. For protocols that
  have eager read paths, for example multiplexed protocols and non-streaming clients,
  a push-based protocol implementation can provide significant performance benefits
  by avoiding the impedance mismatch between the underlying Netty framework and the
  pull-based Transport model. ``PHAB_ID=D189187``

* finagle-core: There is now an implicit instance for Finagle's default timer:
  `DefaultTimer.Implicit`. ``PHAB_ID=D185896``

* finagle-core: Introduce new command-line flag `c.t.f.tracing.enabled` to entirely
  disable/enable tracing for a given process (default: `true`).  ``PHAB_ID=D186557``

* finagle-mux: Promote the push-based Mux implementation out of experimental status.
  ``PHAB_ID=D189187``

* finagle-mysql: `com.twitter.util.Time` can now be used with
  `PreparedStatement`s without converting the `ctu.Time` to a `java.sql.Timestamp`.
  ``PHAB_ID=D182973``

* finagle-stats: Adds a lint rule to detect when metrics with colliding names are used.
  ``PHAB_ID=D183494``

* finagle-core: Client side `NackAdmissionFilter` can now be configured more easily by
  calling `$client.withAdmissionControl.nackAdmissionControl(window, threshold)`.
  ``PHAB_ID=D188877``

* finagle-thrift: Trait c.t.scrooge.ThriftService is now c.t.finagle.thrift.ThriftService.
  Scrooge generated service objects now all inherit from c.t.finagle.thrift.GeneratedThriftService.
  ``PHAB_ID=D180341``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.dispatch.ClientDispatcher.wrapWriteException` has been turned from a
  partial function instance into a static total function. ``PHAB_ID=D189639``

* finagle-mux: `ClientDiscardedRequestException` now extends `FailureFlags` and is no longer
  a case class. ``PHAB_ID=D183456``

Bug Fixes
~~~~~~~~~

* finagle-core: `c.t.f.filter.NackAdmissionFilter` is now aware of `FailureFlags` encoded
  failures. ``PHAB_ID=D193390``

* finagle-mux: Mux's server dispatcher is now aware of `FailureFlags` encoded failures.
  ``PHAB_ID=D193456``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Server-side rejections from `c.t.f.filter.RequestSempahoreFilter.module` are now
  captured by `c.t.f.service.StatsFilter`. They will roll up under "/failures",
  "/failures/rejected", and "/failures/restartable" in stats. ``PHAB_ID=D187127``

* finagle-core: `c.t.f.tracing.Trace.tracers` now returns only distinct tracers stored in
  the local context (returned all tracers before).  ``PHAB_ID=D188389``

* finagle-http: HTTP param decoding is no longer truncated to 1024 params.
  ``PHAB_ID=D190113``

* finagle-mux: When mux propagates an interrupt started by `BackupRequestFilter` over the
  network, the `FailureFlags.Ignorable` status is propagated with it.  ``PHAB_ID=D183456``

18.6.0
------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: By default, the deterministic aperture load balancer doesn't expand
  based on the loadband. This is because the loadband is influenced by a degree of
  randomness, and this breaks the deterministic part of deterministic aperture and
  can lead to aggressive banding on backends. ``PHAB_ID=D180922``

* finagle-http2: Unprocessed streams are retryable in case of GOAWAY.
  ``PHAB_ID=D174401``

New Features
~~~~~~~~~~~~

* finagle-core: Add `PropagateDeadlines` `Stack.Param` to `TimeoutFilter` for
  disabling propagation of deadlines to outbound requests.
  ``PHAB_ID=D168405``

* finagle-core: Add `toString` implementations to `c.t.finagle.Service` and
  `c.t.finagle.Filter`. Update in `Filter#andThen` composition to expose a
  useful `toString` for composed Filters and a composed Service (a Filter chain
  with a terminal Service or ServiceFactory).

  The default implementation for `Filter` and `Service` is `getClass.getName`. When
  composing filters, the `andThen` composition method correctly tracks the composed
  parts to produce a useful `toString`, e.g.,

.. code-block:: scala

  package com.foo

  import com.twitter.finagle.{Filter, Service}
  import com.twitter.util.Future

  class MyFilter1 extends Filter[Int, Int, Int, Int] {
     def apply(request: Int, service: Service[Int, Int]): Future[Int] = ???
  }

.. code-block:: scala

  package com.foo

  import com.twitter.finagle.{Filter, Service}
  import com.twitter.util.Future

  class MyFilter2 extends Filter[Int, Int, Int, Int] {
    def apply(request: Int, service: Service[Int, Int]): Future[Int] = ???
  }

.. code-block:: scala

  val filters = (new MyFilter1).andThen(new MyFilter2)

`filters.toString` would emit the String "com.foo.MyFilter1.andThen(com.foo.MyFilter2)"

If a Service (or ServiceFactory) were then added:

.. code-block:: scala

  import com.twitter.finagle.{Filter, Service}
  import com.twitter.finagle.service.ConstantService
  import com.twitter.util.Future

  ...

  val svc: Service[Int, Int] = filters.andThen(new ConstantService[Int, Int](Future.value(2)))

Then, `svc.toString` would thus return the String:
"com.foo.MyFilter1.andThen(com.foo.MyFilter2).andThen(com.twitter.finagle.service.ConstantService(ConstFuture(2)))"

Filter implementations are permitted to override their `toString` implementations which would
replace the default of `getClass.getName`. ``PHAB_ID=D172526``

* finagle-core: Make `Filter.TypeAgnostic` an abstract class for Java usability.
  ``PHAB_ID=D172716``

* finagle-core: `c.t.f.filter.NackAdmissionFilter` is now public. ``PHAB_ID=D177322``

* finagle-core: Extended `c.t.f.ssl.KeyCredentials` and `c.t.f.ssl.TrustCredentials` to work
  with `javax.net.ssl.KeyManagerFactory` and `javax.net.ssl.TrustManagerFactory` respectively.
  ``PHAB_ID=D177484``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Rename `DeadlineFilter.Param(maxRejectFraction)` to
  `DeadlineFilter.MaxRejectFraction(maxRejectFraction)` to reduce confusion
  when adding additional params.
  ``PHAB_ID=D172402``


Bug Fixes
~~~~~~~~~

* finagle-http2: `StreamTransportFactory` now marks itself as dead/closed when it runs out of
  HTTP/2 stream IDs instead of stalling. This allows the connection to be closed/reestablished in
  accordance with the spec ``PHAB_ID=D175898``

* finagle-netty4: `SslServerSessionVerifier` is now supplied with the proper peer address
  rather than `Address.failing`. ``PHAB_ID=D168334``

* finagle-thrift/thriftmux: Disabled client side per-endpoint stats by default for client
  ServicePerEndpoint. It can be set via `c.t.f.thrift.RichClientParam` or a `with`-method
  as `Thrift{Mux}.client.withPerEndpointStats`. ``PHAB_ID=D169427``

* finagle-netty4: Avoid NoClassDefFoundError if netty-transport-native-epoll is not available
  on the classpath.

18.5.0
------

New Features
~~~~~~~~~~~~

* finagle-base-http: Added ability to add SameSite attribute to Cookies to
  comply with https://tools.ietf.org/html/draft-west-first-party-cookies-07.
  The attribute may be set in the constructor via the `c.t.f.http.Cookie`
  `sameSite` param or via the `c.t.f.http.Cookie.sameSite` method. ``PHAB_ID=D157942``

  - Pass `SameSite.Lax` to the `Cookie` to add the "Lax" attribute.
  - Pass `SameSite.Strict` to the `Cookie` to add the "Strict" attribute.

* finagle-base-http: Introduced an API to extract query string params from a
  `c.t.f.http.Request`, `c.t.f.http.Uri.fromRequest` and `c.t.f.http.Uri#params`.
  ``PHAB_ID=D160298``

* finagle-mysql: Added APIs to `Row` which simplify the common access pattern.
  For example, `Row.stringOrNull(columnName: String): String` and
  `Row.getString(columnName: String): Option[String]`.
  ``PHAB_ID=D156926``, ``PHAB_ID=D157360``

* finagle-mysql: Added `read` and `modify` APIs to `c.t.f.mysql.Client` and
  `c.t.f.mysql.PreparedStatement` for that return the specific type of
  `Result` for those operations, `ResultSet` and `OK` respectively.
  ``PHAB_ID=D160215``

* finagle-serversets: Zk2Session's AsyncSemaphore which controls the maximum
  concurrent Zk operations is configurable (GlobalFlag c.t.f.serverset2.zkConcurrentOperations).
  ```PHAB_ID=D157709```

* finagle-mysql: Address `CursoredStatement` usability from Java via
  `CursoredStatement.asJava()`. Through this, you can use the API with
  varargs and Java 8 lambdas. ``PHAB_ID=D158399``

* finagle-toggle: Improved Java compatibility for `ToggleMap` and `Toggle`. ``PHAB_ID=D164489``

* finagle-toggle: `StandardToggleMap.apply` and `StandardToggleMap.registeredLibraries` now
  use `ToggleMap.Mutable` to better support mutating the underlying mutable `ToggleMap`.
  ``PHAB_ID=D167046``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-mux: With the introduction of the push-based mux client, we've
  removed the need for the optimized `c.t.f.Mux.Netty4RefCountingControl`
  MuxImpl, which has been removed. ``PHAB_ID=D141010``

* finagle-mysql: `c.t.f.mysql.Client.ping` now returns a `Future[Unit]`
  instead of the broad `Future[Result]` ADT. ``PHAB_ID=D160215``

* finagle-toggle: Changed `ToggleMap.Mutable` from a trait to an abstract class, and
  `ToggleMap.Proxy` no longer extends `ToggleMap`, but now has a self-type that conforms to
  `ToggleMap` instead. ``PHAB_ID=D164489``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Add `c.t.f.SslException` to better model exceptions related to SSL/TLS.
  The `c.t.f.ChannelException.apply` method will now wrap `javax.net.ssl.SSLException`s in
  `c.t.f.SslException`. ``PHAB_ID=D158344``

* finagle-core: MethodBuilder metrics now include failures.
  ``PHAB_ID=D167589``, ``PHAB_ID=D168095``

* finagle-http: ServerAdmissionControl is circumvented for HTTP requests that have
  a body unless the request contains the header 'finagle-http-retryable-request' since
  it cannot be known whether the client can actually retry them, potentially resulting
  in depressed success rates during periods of throttling. ``PHAB_ID=D134209``

* finagle-http2: Clients and servers no longer attempt a cleartext upgrade if the
  first request of the HTTP/1.1 session has a body. ``PHAB_ID=D153986``

* finagle-thriftmux: The push-based client muxer is now the default muxer implementation.
  The push-based muxer has better performance and a simpler architecture. ``PHAB_ID=D158134``

* finagle-toggle: `ToggleMap.Proxy#underlying` is now public, and `ToggleMap.Proxy`
  participates in `ToggleMap.components`. ``PHAB_ID=D167046``

Bug Fixes
~~~~~~~~~

* finagle-base-http: Concurrent modification of the `c.t.f.http.DefaultHeaderMap` could
  result in an infinite loop due to HashMap corruption. Access is now synchronized to avoid
  the infinite loop. ``PHAB_ID=D159250``

* finagle-core: `FailureFlags` that have their flags set modified will now
  retain the original stack trace, suppressed Throwables, and cause when possible.
  ``PHAB_ID=D160402``

* finagle-memcached: Added the missing support for partial success for the batch
  operations in the new PartitioningService based Memcached client. ``PHAB_ID=D161249``

* finagle-thrift: Removed copied libthrift files. ``PHAB_ID=D165455``

* finagle-thrift/thriftmux: Server side per-endpoint statsFilter by default is disabled now.
  It can be set via `c.t.f.thrift.RichServerParam` or a `with`-method as
  `Thrift{Mux}.server.withPerEndpointStats`. ``PHAB_ID=D167433``

18.4.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `c.t.f.filter.NackAdmissionFilter` can now be disabled via a `with`-method.
  `$Protocol.client.withAdmissionControl.noNackAdmissionControl` ``PHAB_ID=D146873``

* finagle-mysql: Exceptions now include the SQL that was being executed when possible.
  ``PHAB_ID=D150503``

* finagle-mysql: Address `PreparedStatement` usability from Java via
  `PreparedStatement.asJava()`. Through this, you can use the API with
  varargs and Java 8 lambdas. ``PHAB_ID=D156755``

* finagle-mysql: Added support for `Option`\s to `Parameter` implicits. This
  allows for the natural representation of nullable columns with an `Option`
  where a `None` is treated as a `null`. ``PHAB_ID=D156186``

* finagle-netty4: Add 'tls/connections' gauge for Finagle on Netty 4 which tracks the number
  of open SSL/TLS connections per Finagle client or server.
  ``PHAB_ID=D144184``

* finagle-redis: Support has been added for a number of new cluster commands
  introduced in Redis 3.0.0. ``PHAB_ID=D152186``

Bug Fixes
~~~~~~~~~

* finagle-mysql: Fix handling of interrupts during transactions. ``PHAB_ID=D154441``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.ssl.client.HostnameVerifier` has been removed since it was using
  `sun.security.util.HostnameChecker` which is no longer accessible in JDK 9.
  ``PHAB_ID=D144149``

* finagle-thrift: Upgraded libthrift to 0.10.0, `c.t.f.thrift.Protocols.TFinagleBinaryProtocol`
  constructor now takes `stringLengthLimit` and `containerLengthLimit`, `NO_LENGTH_LIMIT` value
  changed from 0 to -1. ``PHAB_ID=D124620``

* finagle-thrift: Move "stateless" methods in `c.t.finagle.thrift.ThriftRichClient`
  to `c.t.finagle.thrift.ThriftClient`. Then mix the `ThriftClient` trait into the
  ThriftMux and Thrift Client companions to make it clearer that these stateless methods
  are not affected by the changing state of the configured client instance but are instead
  simply utility methods which convert or wrap the incoming argument. ``PHAB_ID=D143185``

* finagle-base-http: Removed deprecated `c.t.f.Cookie.value_=`; use `c.t.f.Cookie.value`
  instead. ``PHAB_ID=D148266``

* finagle-base-http: Removed deprecated `c.t.f.Cookie.domain_=`; use `c.t.f.Cookie.domain`
  instead. ``PHAB_ID=D148266``

* finagle-base-http: Removed deprecated `c.t.f.Cookie.path_=`; use `c.t.f.Cookie.path`
  instead. ``PHAB_ID=D148266``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Add minimum request threshold for `successRateWithinDuration` failure accrual.
  ``PHAB_ID=D154129``

* finagle-core: `c.t.f.filter.NackAdmissionFilter` no longer takes effect when
  the client's request rate is too low to accurately update the EMA value or
  drop requests. ``PHAB_ID=D143996``

* finagle-core: SSL/TLS client hostname verification is no longer performed by
  `c.t.f.ssl.client.HostnameVerifier`. The same underlying library
  `sun.security.util.HostnameChecker` is used to perform the hostname verification.
  However it now occurs before the SSL/TLS handshake has been completed, and the
  exception on failure has changes from a `c.t.f.SslHostVerificationException` to a
  `javax.net.ssl.CertificateException`. ``PHAB_ID=D144149``

* finagle-core: Closing `c.t.f.NullServer` is now a no-op. ``PHAB_ID=D156098``

* finagle-netty4: Netty ByteBuf leak tracking is enabled by default. ``PHAB_ID=D152828``

Deprecations
~~~~~~~~~~~~

* finagle-thrift: System property "-Dorg.apache.thrift.readLength" is deprecated. Use
  constructors to set read length limit for TBinaryProtocol.Factory and TCompactProtocol.Factory.
  ``PHAB_ID=D124620``

18.3.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `c.t.f.client.BackupRequestFilter.filterService` for wrapping raw services in a
  `c.t.f.client.BackupRequestFilter` is now public. ``PHAB_ID=D135484``

* finagle-core: Introduce `c.t.f.Stacks.EMPTY_PARAMS` for getting an empty Param map from
  Java, and `c.t.f.Stack.Params.plus` for easily adding Params to a Param map from Java.
  ``PHAB_ID=D139660``

Bug Fixes
~~~~~~~~~

* finagle-core: `c.t.f.liveness.FailureAccrualFactory` takes no action on `c.t.f.Failure.Ignorable`
  responses. ``PHAB_ID=D135435``

* finagle-core: `c.t.f.pool.WatermarkPool` is resilient to multiple closes on a service instance.
  ``PHAB_ID=D137198``

* finagle-core: `c.t.f.pool.CachingPool` service wrapper instances are resilient to multiple closes.
  ``PHAB_ID=D136781``

* finagle-core: Requeue module now closes sessions it prevented from propagating up the stack.
  ``PHAB_ID=D142457``

* finagle-base-http: `c.t.f.http.Netty4CookieCodec.encode` now wraps Cookie values that would
  be wrapped in `c.t.f.http.Netty3CookieCodec.encode`. ``PHAB_ID=D134566``

* finagle-base-http: `c.t.f.http.Cookie.maxAge` returns `c.t.f.http.Cookie.DefaultMaxAge`
  (instead of null) if maxAge has been set to null or None in the copy constructor
  ``PHAB_ID=D138393``.

* finagle-http: The HTTP client will not attempt to retry nacked requests with streaming
  bodies since it is likely that at least part of the body was already consumed and therefore
  it isn't safe to retry. ``PHAB_ID=D136053``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: Removed `c.t.f.http.Cookie.comment_`, `c.t.f.http.Cookie.comment_=`,
  `c.t.f.http.Cookie.commentUrl_`, and `c.t.f.http.Cookie.commentUrl_=`. `comment` and `commentUrl`
  per RFC-6265. ``PHAB_ID=D137538``

* finagle-base-http: Removed deprecated `c.t.f.http.Cookie.isDiscard` and
  `c.t.f.http.Cookie.isDiscard_=`, per RFC-6265. ``PHAB_ID=D138109``

* finagle-base-http: Removed deprecated `c.t.f.http.Cookie.ports` and
  `c.t.f.http.Cookie.ports_=`, per RFC-6265. ``PHAB_ID=D139243``

* finagle-base-http: `c.t.f.http.RequestBuilder` has been moved to the finagle-http target
  and the implicit evidence, `RequestConfig.Yes` has been renamed to `RequestBuilder.Valid`.
  ``PHAB_ID=D122227``

* finagle-base-http: Removed deprecated `c.t.f.Cookie.isSecure`; use `c.t.f.Cookie.secure`
  instead. Removed deprecated `c.t.f.Cookie.isSecure_=`. ``PHAB_ID=D140435``

* finagle-base-http: Removed deprecated `c.t.f.http.Cookie.version` and
  `c.t.f.http.Cookie.version_=`, per RFC-6265. ``PHAB_ID=D142672``

* finagle-base-http: Removed deprecated `c.t.f.Cookie.httpOnly_=`; use `c.t.f.Cookie.httpOnly`
  instead. ``PHAB_ID=D143177``

* finagle-base-http: Removed deprecated `c.t.f.Cookie.maxAge_=`; use `c.t.f.Cookie.maxAge`
  instead. ``PHAB_ID=D143177``

* finagle-core: `c.t.f.pool.WatermarkPool` was finalized. ``PHAB_ID=D137198``

* finagle-core: `c.t.finagle.ssl.Ssl` and related classes have been
  removed. They were replaced as the primary way of using SSL/TLS
  within Finagle in release 6.44.0 (April 2017). Please migrate to using
  `c.t.f.ssl.client.SslClientEngineFactory` or
  `c.t.f.ssl.server.SslServerEngineFactory` instead. ``PHAB_ID=D135908``

* finagle-core: Removed `newSslEngine` and `newFinagleSslEngine` from
  `ServerBuilder`. Please implement a class which extends
  `c.t.f.ssl.server.SslServerEngineFactory` with the previously passed in
  function used as the implementation of the `apply` method. Then use the
  created engine factory with one of the `tls` methods instead.
  ``PHAB_ID=D135908``

* finagle-core: The deprecated `c.t.f.loadbalancer.DefaultBalancerFactory` has been removed.
  ``PHAB_ID=D139814``

* finagle-exp: The deprecated `c.t.f.exp.BackupRequestFilter` has been removed. Please use
  `c.t.f.client.BackupRequestFilter` instead. ``PHAB_ID=D143333``

* finagle-http: Removed the `c.t.f.Http.Netty3Impl`. Netty4 is now the only
  underlying HTTP implementation available. ``PHAB_ID=D136705``

* finagle-zipkin-scribe: Renamed the finagle-zipkin module to finagle-zipkin-scribe, to
  better advertise that this is just the scribe implementation, instead of the default.
  ``PHAB_ID=D141940``

18.2.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Add orElse to allow composition of `FailureAccrualPolicy`s.
  ``PHAB_ID=D131156``

* finagle-core: `c.t.f.http.MethodBuilder` now exposes a method `newService` without a
  `methodName` to create a client. `c.t.f.thriftmux.MethodBuilder` now exposes a
  method `servicePerEndpoint` without a `methodName` to create a client. ``PHAB_ID=D131809``

* finagle-thriftmux: Expose the underlying configured client `label` in the
  `c.t.finagle.thriftmux.MethodBuilder`. ``PHAB_ID=D129109``

Bug Fixes
~~~~~~~~~

* finagle-http2: http2 servers no longer leak ping bodies. ``PHAB_ID=D130503``

Deprecations
~~~~~~~~~~~~

* finagle-core: `c.t.finagle.ssl.Ssl` and related classes have been
  deprecated. They were replaced as the primary way of using SSL/TLS
  within Finagle in release 6.44.0 (April 2017). Please migrate to using
  `c.t.f.ssl.client.SslClientEngineFactory` or
  `c.t.f.ssl.server.SslServerEngineFactory` instead. ``PHAB_ID=D129692``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: `c.t.f.h.codec.HttpCodec` has been moved to the `finagle-http`
  project. ``PHAB_ID=D116364``

* finagle base-http: `c.t.f.h.Request.multipart` has been removed.
  Use `c.t.f.h.exp.MultipartDecoder` instead. ``PHAB_ID=D129158``

* finagle-http: Split the toggle 'c.t.f.h.UseH2C' into a client-side toggle and a
  server-side toggle, named 'c.t.f.h.UseH2CClients', and 'c.t.f.h.UseH2CServers',
  respectively.  ``PHAB_ID=D130988``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Finagle clients with retry budgets or backoffs should no
  longer have infinite hash codes. ``PHAB_ID=D128594``

* finagle-core: `c.t.f.l.Balancer` no longer uses a `c.t.f.u.Updater` as its underlying
  concurrency primitive as it was found that in practice coalescing updates almost never
  happens and in the absence of that `Updater` imposes more overhead than simple
  synchronization while complicating the result of calling `rebuild()` since we don't know
  if the rebuild actually occurred by the time we attempt to use the distributor again.
  ``PHAB_ID=D126486``

18.1.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `FailureDetector` has a new method, `onClose`, which provides
  a Future that is satisfied when the `FailureDetector` marks a peer as Closed.
  ``PHAB_ID=D126840``

* finagle-core: Introduce trace logging of requests as they flow through a
  Finagle client or server. These logs can be turned on at runtime by setting
  the "com.twitter.finagle.request.Logger" logger to trace level.
  ``PHAB_ID=D124352``

* finagle-http2: HTTP/2 clients now expose the number of currently opened streams under
  the `$client/streams` gauge. ``PHAB_ID=D127238``

* finagle-http2: HTTP/2 servers now expose the number of currently opened streams under
  the `$server/streams` gauge. ``PHAB_ID=D127667``

* finagle-memcached: By default, the Memcached client now creates two connections
  to each endpoint, instead of 4. ``PHAB_ID=D119619``

* finagle-redis: Add support for redis Geo Commands. ``PHAB_ID=D123167`` based on the PR
  https://github.com/twitter/finagle/pull/628 written by Mura-Mi [https://github.com/Mura-Mi]

* finagle-thrift: Add `c.t.f.thrift.service.ThriftServiceBuilder` and
  `c.t.f.thrift.service.ReqRepThriftServiceBuilder` for backwards compatibility
  of creating higher-kinded method-per-endpoint clients. ``PHAB_ID=D127538``

* finagle-core: `c.t.f.http.MethodBuilder` and `c.t.f.thriftmux.MethodBuilder` now
  expose `idempotent` and `nonIdempotent` methods, which can be used to configure
  retries and the sending of backup requests. ``PHAB_ID=D122087``

Bug Fixes
~~~~~~~~~

* finagle-mysql: Fix a bug with transactions where an exception during a rollback
  could leave the connection with a partially committed transaction. ``PHAB_ID=D122771``

* finagle-toggle: `c.t.f.toggle.Toggle`s are independent; that is, applying the same value to
  two different toggles with the same fraction will produce independent true/false
  values. ``PHAB_ID=D128172``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core, finagle-netty4: When creating engines, SslClientEngineFactories now use
  `SslClientEngineFactory.getHostString` instead of `SslClientEngineFactory.getHostname`.
  This no longer performs an unnecessary reverse lookup when a hostname is not supplied
  as part of the `SslClientConfiguration`.  ``PHAB_ID=D124369``

* finagle-http2: Supplies a dependency on io.netty.netty-tcnative-boringssl-static,
  which adds support for ALPN, which is necessary for encrypted http/2.  To use a
  different static ssl dependency, exclude the tcnative-boringssl dependency and
  manually depend on the one you want to use. ``PHAB_ID=D119555``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http, finagle-http: Removed Apache Commons Lang dependency,
  `org.apache.commons.lang3.time.FastDateFormat` now is `java.time.format.DateTimeFormatter`.
  ``PHAB_ID=D121479``

* finagle-base-http: `c.t.f.http.Message.headerMap` is now an abstract method.
  ``PHAB_ID=D120931``

* finagle-core: `c.t.f.ssl.server.SslServerSessionVerifier` no longer uses the unauthenticated
  host information from `SSLSession`. ``PHAB_ID=D124815``

* finagle-memcached: `ConcurrentLoadBalancerFactory` was removed and its behavior
  was replaced by a Stack.Param inside finagle-core's `LoadBalancerFactory`.
  ``PHAB_ID=D119394``

* finagle-netty4: `Netty4ClientEngineFactory` and `Netty4ServerEngineFactory` were finalized.
  ``PHAB_ID=D128708``

* finagle-thrift, finagle-thriftmux: Remove `ReqRep` specific methods. Since the "ReqRep"
  builders are now subclasses of their non-"ReqRep" counterparts their is no longer a
  need to expose "ReqRep" specific methods. ``PHAB_ID=D123341``

Deprecations
~~~~~~~~~~~~

* finagle-exp: `c.t.f.exp.BackupRequestFilter` has been deprecated. Please use
  `c.t.f.client.BackupRequestFilter` instead. ``PHAB_ID=D122344``

* finagle-http: `c.t.f.http.Request.multipart` has been deprecated.
  Use `c.t.f.http.exp.MultipartDecoder` instead. ``PHAB_ID=D126013``

17.12.0
-------

New Features
~~~~~~~~~~~~

* finagle-core: Expose Tunables for MethodBuilder timeout configuration. Update
  the http.MethodBuilder and thriftmux.MethodBuilder to accept Tunables for
  configuring total and per-request timeouts. ``PHAB_ID=D118114``

* finagle-thrift, finagle-thriftmux: Add support for Scrooge
  `ReqRepServicePerEndpoint` functionality. ``PHAB_ID=D107397``

* finagle-thriftmux: Add support for Scrooge `ServicePerEndpoint` and
  `ReqRepServicePerEndpoint` functionality to `thriftmux.MethodBuilder`.
  ``PHAB_ID=D116081``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: Remove deprecated `[Request|Response].[encode|decode][Bytes|String]`
  methods. Use c.t.f.h.codec.HttpCodec methods instead. ``PHAB_ID=D116350``

* finagle-memcached: `ConcurrentLoadBalancerFactory` was removed and its behavior
  was replaced by a Stack.Param inside finagle-core's `LoadBalancerFactory`.
  ``PHAB_ID=D119394``

* finagle-serversets: Removed Guava dependency which broke some APIs. ``PHAB_ID=D119555``

  - `c.t.f.common.zookeeper.ServerSets.TO_ENDPOINT` is now a `java.util.function.Function`.
  - `c.t.f.common.net.pool.DynamicHostSet.HostChangeMonitor.onChange` now takes a `java.util.Set`.
  - `c.t.f.common.zookeeper.ZooKeeperUtils.OPEN_ACL_UNSAFE` is is now a `java.util.List`.
  - `c.t.f.common.zookeeper.ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL` is is now a `java.util.List`.
  - `c.t.f.common.zookeeper.ZooKeeperClient` constructor now takes a `java.util.Optional`.

* finagle-thrift: Move `ThriftRichClient` and `ThriftRichServer` to
  `c.t.finagle.thrift` package. ``PHAB_ID=D115284``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Remove `NackAdmissionControl` from the default client stack.
  Add it to the finagle-{http,mux} client stacks; note that it is added to
  finagle-http2 via finagle-http and finagle-thriftmux via finalge-mux. It is
  no longer part of the finagle-{memcached,mysql,redis} client stacks.
  ``PHAB_ID=D116722``

* finagle-core: The "pipelining/pending" stat has been removed from protocols
  using `c.t.f.dispatch.PipeliningClientDispatcher`. Refer to the "pending" stat
  for the number of outstanding requests. ``PHAB_ID=D113424``

* finagle-thrift,thriftmux: Tracing of RPC method names has been removed. This
  concern has moved into Scrooge. ``PHAB_ID=D115294``

Deprecations
~~~~~~~~~~~~

* finagle-core: `c.t.f.BackupRequestLost` has been deprecated. Please use a
  `c.t.f.Failure` flagged `c.t.f.Failure.Ignorable` instead. ``PHAB_ID=D113466``

17.11.0
-------

New Features
~~~~~~~~~~~~

* finagle-core: Add `ResponseClassifier`s, RetryOnTimeout and RetryOnChannelClosed,
  for exceptions that are commonly retried when building from ClientBuilder but had
  no MethodBuilder equivalents. ``PHAB_ID=D106706``

* finagle-netty4: `Netty4Transporter` and `Netty4Listener` are now accessible, which
  allows external users to create their own protocols for use with Finagle on Netty 4.
  ``PHAB_ID=D105627``

Bug Fixes
~~~~~~~~~

* finagle-exp: Fix race condition in `LatencyHistogram` which could lead to the wrong
  value returned for `quantile`. ``PHAB_ID=D106330``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Numerous overloads of `c.t.f.Server.serve` have been marked final.
  ``PHAB_ID=D107280``

* finagle-thrift: Correctly send `mux.Request#contexts` in all cases. There were some
  cases in which `mux.Request#contexts` were not always propagated. The contexts are
  now always written across the transport. Note that there may be duplicated contexts
  between "local" context values and "broadcast" context values. Local values will
  precede broadcast values in sequence. ``PHAB_ID=D107921``

17.10.0
-------

Release Version Format
~~~~~~~~~~~~~~~~~~~~~~

* From now on, release versions will be based on release date in the format of
  YY.MM.x where x is a patch number. ``PHAB_ID=D101244``

New Features
~~~~~~~~~~~~

* finagle-core: DeadlineFilter may now be created from the class and used as a
  regular Filter in addition to a stack module as before. ``PHAB_ID=D94517``

* finagle-mysql: Add ability to toggle the `CLIENT_FOUND_ROWS` flag. ``PHAB_ID=D91406``

* finagle-http: Separated the DtabFilter.Extractor from the ServerContextFilter into
  a new module: ServerDtabContextFilter. While this is still enabled in the default
  Http server stack, it can be disabled independently of the ServerContextFilter.
  ``PHAB_ID=D94306``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-netty4: `Netty4ClientEngineFactory` and `Netty4ServerEngineFactory` now
  validate loaded certificates in all cases to ensure that the current date
  range is within the validity range specified in the certificate. ``PHAB_ID=D88664``

* finagle-netty4: `TrustCredentials.Insecure` now works with native SSL/TLS engines.
  ``PHAB_ID=D103766``

* finagle-http2: Upgraded to the new netty http/2 API in netty version 4.1.16.Final,
  which fixes several long-standing bugs but has some bugs around cleartext http/2.
  One of the work-arounds modifies the visibility of a private field, so it's incompatible
  with security managers.  This is only true for http/2--all other protocols will be unaffected.
  ``PHAB_ID=D98069``

* finagle-http: Netty 3 `HeaderMap` was replaced with our own implementation.
  ``PHAB_ID=D99127``

Deprecations
~~~~~~~~~~~~

* finagle-base-http: With the intention to make `c.t.f.http.Cookie` immutable,
  `set` methods on `c.t.f.http.Cookie` have been deprecated:

    - `comment_=`
    - `commentUrl_=`
    - `domain_=`
    - `maxAge_=`
    - `path_=`
    - `ports_=`
    - `value_=`
    - `version_=`
    - `httpOnly_=`
    - `isDiscard_=`
    - `isSecure_=`

  Use the `c.t.f.http.Cookie` constructor to set `domain`, `maxAge`, `path`, `value`, `httpOnly`,
  and `secure`. `comment`, `commentUrl`, `ports`, `version`, and `discard` have been removed
  per RFC-6265. ``PHAB_ID=D82164``.

  Alternatively, use the `domain`, `maxAge`, `path`, `httpOnly`, and `secure` methods to create a
  new `Cookie` with the existing fields set, and the respective field set to a given value.
  ``PHAB_ID=D83226``

* finagle-base-http: `c.t.f.http.Cookie.isSecure` and `c.t.f.http.Cookie.isDiscard`
  have been deprecated. Use `c.t.f.http.Cookie.secure` for `c.t.f.http.Cookie.isSecure`.
  `isDiscard` has been removed per RFC-6265. ``PHAB_ID=D82164``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-mysql: Moved `Cursors.cursor` method to `Client` trait, and removed `Cursors` trait.
  This allows cursor queries to used with transactions.  ``PHAB_ID=D91789``

* finagle-mux: Expose transport contexts in mux.Request and mux.Response. ``PHAB_ID=D92998``

* finagle-mux: The "leased" gauge has been removed from the mux client implementation since the
  metric is reported as the sum of the value over all clients which is unlikely to be useful.
  ``PHAB_ID=D100357``

7.1.0
-----

New Features
~~~~~~~~~~~~

* finagle-core: If a `c.t.u.tunable.Tunable` request or total timeout has been configured
  on a client which uses a `c.t.f.client.DynamicTimeout` filter, the current value of tunable will
  be used in the case of no dynamic timeout set for a request. ``PHAB_ID=D81886``

* finagle-core: `FailFastException` now captures the throwable that caused it. ``PHAB_ID=D86396``

* finagle-redis: finagle interface for redis DBSIZE command. ``PHAB_ID=D85305``

Bug Fixes
~~~~~~~~~

* finagle-core: Unregister `ServerRegistry` entry on `StackServer#close`. A
  StackServer entry is registered in the `ServerRegistry` on serve of the
  server but never unregistered. It is now unregistered on close of
  the StackServer. ``PHAB_ID=D83200``

* finagle-mux: Fix two issues with mux leases. In one bug, a new lease wouldn't be sent to
  the client if it was issued within 1 second of when the existing lease was set to expire.
  In a second bug, the server would only nack if the issued lease was 0, but didn't consider
  whether the lease had expired. ``PHAB_ID=D91645``


* finagle-netty4: `Netty4ClientEngineFactory` and `Netty4ServerEngineFactory` now
  properly load all chain certificates when the `SslClientConfiguration` or
  `SslServerConfiguration` uses `KeyCredentials.CertKeyAndChain` instead of just the
  first one in the file. ``PHAB_ID=D82414``

* finagle-thrift/thriftmux: Thrift/ThriftMux servers and clients now can be configured
  with `withMaxReusableBufferSize` to specify the max size of the reusable buffer for
  Thrift responses. ``PHAB_ID=D83190``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-stats: Verbosity levels are now respected: debug-metrics aren't exported
  by default.  ``PHAB_ID=D85278``

* finagle-netty4: `ChannelTransport` no longer considers the `Channel.isWritable` result
  when determining status. ``PHAB_ID=D82670``

Deprecations
~~~~~~~~~~~~

* finagle-base-http: Encoding/decoding methods on `c.t.f.http.Request` and `c.t.f.http.Response`
  to/from Strings and arrays of bytes have been deprecated. Use the methods on
  `c.t.f.http.codec.HttpCodec` instead:

     - For `c.t.f.http.Request.encodeString`, use `c.t.f.Http.codec.HttpCodec.encodeRequestToString`
     - For `c.t.f.http.Request.encodeBytes`, use `c.t.f.Http.codec.HttpCodec.encodeRequestToBytes`
     - For `c.t.f.http.Request.decodeString`, use `c.t.f.Http.codec.HttpCodec.decodeStringToRequest`
     - For c.t.f.http.Request.decodeBytes`, use `c.t.f.Http.codec.HttpCodec.decodeBytesToRequest`
     - For `c.t.f.http.Response.encodeString`, use `c.t.f.Http.codec.HttpCodec.encodeResponseToString`
     - For `c.t.f.http.Response.decodeString`, use `c.t.f.Http.codec.HttpCodec.decodeStringToResponse`
     - For `c.t.f.http.Response.decodeBytes`, use `c.t.f.Http.codec.HttpCodec.decodeBytesToResponse`

  ``PHAB_ID=D81341``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Remove deprecated method `httpProxyTo(String, Option[Transporter.Credentials])`.
  Use `httpProxyTo(String, Transporter.Credentials]` instead. ``PHAB_ID=D84077``

* finagle-\*-http: Netty 3 specific HTTP transport related code has been moved into its own
  project, finagle-netty3-http, in preparation for removing it from Finagle.
  ``PHAB_ID=D84101``

* finagle-memcached: Remove deprecated method `BaseClient.release()`. Use
  `BaseClient.close()` instead. ``PHAB_ID=D83168``

Deprecations
~~~~~~~~~~~~

* finagle-memcached: Move `c.t.f.memcached.java.Client` to `c.t.f.memcached.JavaClient`,
  `c.t.f.memcached.java.ClientBase` to `c.t.f.memcached.JavaClientBase`, and
  `c.t.f.memcached.java.ResultWithCAS` to `c.t.f.memcached.ResultWithCAS`. ``PHAB_ID=D83719``

* finagle-core: Added a new type member `Context` to `Transport`, and a method that
  returns a context, which has most of the methods currently directly on `Transport`.
  Also deprecates most of those methods--please start using the context instead of the
  `Transport` directly.  Also added type parameters to `Transporter`, `Listener`, and
  type members to `StackClient` and `StackServer`.  ``PHAB_ID=D83972``

* finagle-core: `com.twitter.finagle.loadbalancer.DeterministicOrdering` was renamed
  to `com.twitter.finagle.loadbalancer.ProcessCoordinate` and the internal `Coord` ADT
  was changed as well. ``PHAB_ID=D84452``

* finagle-thrift: Move `Thrift.Server.param.MaxReusableBufferSize` to
  `Thrift.param.MaxReusableBufferSize` for both server and client use. ``PHAB_ID=D83190``

7.0.0
-----

New Features
~~~~~~~~~~~~

* finagle-core: A `StackClient` can be configured with a `c.t.u.tunable.Tunable`
  request timeout using `.withRequestTimeout(tunable)`; this facilitates changing
  the timeout at runtime, without server restart.
  See https://twitter.github.io/finagle/guide/Configuration.html#tunables for details.
  ``PHAB_ID=D80751``.

* finagle-core: `SslClientSessionVerifier` and `SslServerSessionVerifier` have been added
  as `Stack` params for executing custom SSL/TLS `Session` verification logic on the
  establishment of an SSL/TLS `Session`. ``PHAB_ID=D63256``

* finagle-core: `tls` methods which take an `SslClientSessionVerifier` have
  been added to `ClientBuilder` and `ClientTransportParams`
  (withTransport.tls). `tls` methods which take an `SslServerSessionVerifier`
  have been added to `ServerBuilder` and `ServerTransportParams`
  (withTransport.tls). ``PHAB_ID=D68645``

* finagle-core: Timer tasks submitted to the `c.t.f.util.DefaultTimer` can have their
  execution time monitored. Slow executing tasks may result in a log message at level WARN
  and a counter of slow tasks is kept under `finagle/timer/slow`. This can be enabled using
  the global flag `c.t.f.util.defaultTimerProbeSlowTasks` and the maximum allowed runtime
  and minimum duration between log messages can be tuned using the global flags
  `c.t.f.util.defaultTimerSlowTaskMaxRuntime`, and
  `c.t.f.util.defaultTimerSlowTaskLogMinInterval`, respectively. ``PHAB_ID=D70279``

* finagle-core: The JVM metrics for GC, allocations, memory, and more have moved
  here from TwitterServer. See the new JVM section in the user guide for details:
  https://twitter.github.io/finagle/guide/Metrics.html
  ``PHAB_ID=D80883``

* finagle-http, finagle-thriftmux: `MethodBuilder` has been promoted out of experimental.
  `MethodBuilder` is a collection of APIs for client configuration at a higher level than
  the Finagle 6 APIs while improving upon the deprecated `ClientBuilder`.
  See the user guide for details: https://twitter.github.io/finagle/guide/MethodBuilder.html
  ``PHAB_ID=D60032``

* finagle-http: add `withNoAutomaticContinue` api to disable automatically sending 100 CONTINUE
  responses. ``PHAB_ID=D80017``

* finagle-http: The nack related logic in the `c.t.f.h.c.HttpClientDispatcher` has been
  moved into a filter, `c.t.f.h.f.ClientNackFilter` which has been added to the client
  stack and can now be removed based on its `Stack.Role`. ``PHAB_ID=D78902``

* finagle-init: Introduce a module to support service-loading initialization
  code. ``PHAB_ID=D75950``

* finagle-memcached: Added support for partitioned backends in finagle client. Introducing
  the new PartitioningService (``PHAB_ID=D75143``), KetamaPartitioningService (``PHAB_ID=D77499``)
  and MemcachedPartitioningService (``PHAB_ID=D78927``), which provide this support at different
  levels of abstraction. The c.t.f.Memcached util, that is used for creating new memcached
  clients, now creates a new partitioning client that utilizes these new services for the
  Memcached protocol. The new memcached client can be enabled by setting the toggle
  "com.twitter.finagle.memcached.UsePartitioningMemcachedClient" to 1.0. ``PHAB_ID=D80352``

* finagle-mux: Default to new more efficient decoder. ``PHAB_ID=D80225``

* finagle-mysql: `IsolationLevel` support was added with
  `Transactions.transactionWithIsolation` method, so the default level can be overridden
  at the transaction level. ``PHAB_ID=D68944``

* finagle-mysql: Add support for unsigned integers. When enabled, unsigned integers that do
  not fit into the existing signed representation are widened. For example an unsigned
  Int32 is represented as a Java Long, etc. Because this changes the `c.t.f.mysql.Value`
  variant returned by the row, it is disabled by default and must be enabled with the param
  `c.t.f.Mysql.param.UnsignedColumns`. ``PHAB_ID=D78721``

* finagle-netty4: Adds support for passing a chain file to the default TLS implementation.
  ``PHAB_ID=D59531``

* finagle-netty4: Netty 4 transports now use pooled allocators by default. ``PHAB_ID=D75014``

* finagle-netty4: `KeyCredentials.CertKeyAndChain` is now available to use with
  `Netty4ServerEngineFactory`. ``PHAB_ID=D80494``

* finagle-netty4: `c.t.f.netty4.trackReferenceLeaks` is now a CLI flag (default: disabled)
  rather than a toggle. ``PHAB_ID=D80654``

* finagle-stats: Metrics now report verbosity levels via `MetricsView.verbosity`.
  ``PHAB_ID=D78150``

* finagle-stats: `JsonExporter` now respects verbosity levels (current default behavior is
  to keep exporting "debug" metrics). Adjust `com.twitter.finagle.stats.verbose` tunable
  allowlist to change it.  ``PHAB_ID=D79571``

* finagle-tunable: `StandardTunableMap` is now public. Users can access file-based, in-memory,
  and service-loaded tunable values using the map.
  See https://twitter.github.io/finagle/guide/Configuration.html#tunables for details.
  ``PHAB_ID=D80751``.

* finagle: Changed dependencies of Netty from 4.1.10 to 4.1.12. ``PHAB_ID=D60438``

Bug Fixes
~~~~~~~~~

* finagle-mysql: Fix decoding error for medium length integers. ``PHAB_ID=D78505``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: Finagle is now decoupled from Netty 3. Depend on `finagle-netty3`
  explicitly if needed. ``PHAB_ID=D65268``

* finagle-base-http: The HTTP message model has been refactored to remove backing
  Netty 3 types. Additionally, the `Request` and `Response` classes now have private
  constructors to enforce a more appropriate inheritance model: `Request.Proxy` and
  `Response.Proxy` are now the point of entry for extending the HTTP model types. Along
  with the model changes the InputStream generated `.getInputStream()` method of HTTP
  messages no longer consumes the messages body. ``PHAB_ID=D74519``

* finagle-core: The Framer type has been transformed into a specialized version of a
  more generic abstraction, Decoder[T]. ``PHAB_ID=D59495``

* finagle-core: Replace the `c.t.f.context.RemoteInfo.Available` constructor
  which takes `ClientId` in favor of a version taking `String`. `ClientId` is
  Twitter's Thrift specific concept and this should be more generic.
  ``PHAB_ID=D60136``

* finagle-core: Remove the ability to set a global address sort. This is no longer
  necessary as setting this per client is sufficient. ``PHAB_ID=D60698``

* finagle-core: Remove global flag `com.twitter.finagle.tracing.debugTrace`.
  This functionality is better suited as a concrete `Tracer` implementation instead
  of mixed into the generic code. ``PHAB_ID=D63252``

* finagle-core: ``PHAB_ID=D63526``

  - `ClientBuilder.codec` and `ServerBuilder.codec` have been removed. Use `.stack` instead.
  - `ClientBuilder.channelFactory` and `ServerBuilder.channelFactory` have been removed.
     Use `.stack` instead.

* finagle-core: LoadBalancerFactory now takes `Stack.Params` which allows a client to
  more easily pass in the stack context. ``PHAB_ID=D73129``

* finagle-memcached: Add `c.t.util.Closable` trait to `c.t.f.memcached.BaseClient`.
  ``PHAB_ID=D63970``

* finagle-mysql: A number of implementation details were made private such as specific
  `Row` implementations and `ResultSet` builder functions that consume raw packets.
  ``PHAB_ID=D78721``

* finagle-netty4-http: HTTP/1.1 implementation based on Netty 4 is no longer experimental
  and is moved out of the `exp` package. ``PHAB_ID=D80181``

* finagle-serversets: Remove `ZkMetaData.AddressOrdering``, it is no longer used.
  ``PHAB_ID=D60698``

* finagle-stats: `c.t.f.stats.MetricsStatsReceiver` no longer has constructor variants
  which take a `c.t.u.events.Sink` as util-events is now deprecated. ``PHAB_ID=D64437``

* finagle-thrift: The Netty3 thrift implementation has been removed.
  ``PHAB_ID=D63670``

* finagle-zipkin-core: `c.t.f.zipkin.core.SamplingTracer` no longer has constructor
  which takes a `c.t.u.events.Sink` as util-events is now deprecated. ``PHAB_ID=D64437``

* finagle-zipkin: Zipkin Tracer now exports only three counters: `requests`, `failures`,
  `success`.  ``PHAB_ID=D71965``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: The `AsyncSemaphore` which sequences dispatches in `GenSerialClientDispatcher`
  is now failed with a retryable `Failure` so that the retry logic knows that requests that
  which failed to acquire the semaphore are safe to retry. ``PHAB_ID=D78904``

* finagle-http: `serverErrorsAsFailuresV2` toggle is turned into a flag `serverErrorsAsFailures`.
  ``PHAB_ID=D73265``

* finagle-http: Dispatcher stats are now exported under the client scope like
  all other client stats.``PHAB_ID=D72265``

* finagle-http: It's now possible to send a response from the HTTP server that has a
  Content-Length header so long as the 'Transfer-Encoding: chunked' isn't set on the response.
  ``PHAB_ID=D80087``

* finagle-http: Non-streaming servers strip 'expect' headers when a 100 CONTINUE
  response is sent. ``PHAB_ID=D80017``

* finagle-serversets: `Stabilizer` is no longer exporting `pending_tasks` and `deviation_ms`
  stats. See `notify_ms` instead.  ``PHAB_ID=D65571``

* finagle-stats, finagle-zipkin-core: No longer publishing `c.t.u.events` as util-events
  is now deprecated. ``PHAB_ID=D64437``

* finagle-stats: No longer backed by commons metrics, now its own thing.  ``PHAB_ID=D73497``

* finagle-netty4: Unset Netty's default timeout (10 seconds) for SSL handshake on clients.
  Use `.withSession.acquisitionTimeout` instead.  ``PHAB_ID=D78500``

6.45.0
------

New Features
~~~~~~~~~~~~

* finagle: Changed dependencies of Netty from 4.1.9 to 4.1.10 and tcnative
  from 2.0.0 to 2.0.1. ``RB_ID=916056``

* finagle-core: `c.t.f.n.ssl.SslConnectHandler` is no longer exported publicly.
  It has also been renamed to `c.t.f.n.ssl.client.SslClientConnectHandler`.
  ``RB_ID=916932``

* finagle-core: c.t.f.factory.ServiceFactoryCache is now exported publicly.
  ``RB_ID=915064``

* finagle-core: Allow customization of load balancer behavior when no nodes
  are `Status.Open`. See the user guide for details:
  https://twitter.github.io/finagle/guide/Clients.html#behavior-when-no-nodes-are-available
  ``RB_ID=916145``

* finagle-core: The global `c.t.f.naming.NameInterpreter` can be optionally set using
  service loader. ``RB_ID=917082``

* finagle-redis: Support scanning over sets and sorted sets with SSCAN and ZSCAN.
  ``RB_ID=916484``

Bug Fixes
~~~~~~~~~

* finagle-mux: Disable Netty4RefCountingControl decoder when message fragmentation
  is enabled. ``PHAB_ID=D58153``

* finagle: Fixed Java API for `withStack` for Client and Server implementations.
  Java users now get the correct types for calls such as `c.t.f.Http.client().withStack`
  and `c.t.f.Http.server().withStack`. ``RB_ID=915440``

* finagle-thrift, finagle-thriftmux: Clients created using `newServiceIface` now use the
  configured `c.t.f.service.ResponseClassifier` (or `c.t.f.service.ResponseClassifier.Default` if
  not configured) for per-method stats and usage in `c.t.f.liveness.FailureAccrualFactory` and
  `c.t.f.stats.StatsFilter`. ``RB_ID=917010``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle: Add a floor of 8 to the default values of the flags `c.t.f.netty3.numWorkers`
  and `c.t.f.netty4.numWorkers`. ``RB_ID=916465``

* finagle-core: `c.t.f.util.DefaultTimer` is decoupled from Netty 3 and is loaded via the
  `LoadService` machinery. If no timers are available on the class path, the `JavaTimer`
  instead is used instead. This ony affects direct usages of `DefaultTimer` as all Finagle
  protocols are using Netty 4 `HashedWheelTimer` at this point. ``RB_ID=915924``

* finagle-core: The load balancer implementations no longer close the endpoint
  resources when they are closed. Instead, they treat them as externally
  managed resources and expect the layers above to manage them. No change
  is required if using the Balancers in the context of a Finagle client.
  If that's not the case, however, managing the life cycle of the passed
  in endpoints is necessary. ``RB_ID=916415``

* finagle-core: Aperture load balancers now expire idle sessions which fall
  out of the aperture window. ``RB_ID=916508``

* finagle-http: Uses Netty 4 as the default transport implementation.
  Use `.configured(Http.Netty3Impl)` to switch implementation over to Netty 3.
  ``PHAB_ID=D58698`` ``RB_ID=917936``

* finagle-memcached: If the client decoder detects a protocol failure, the ClientTransport
  will close the connection. ``RB_ID=917685``

* finagle-netty4: `poolReceiveBuffers` toggle is removed (suppressed by `UsePooling`).
  ``RB_ID=917912``

* finagle-http: To conform to RFC 2616, a message body is NO LONGER sent when 1xx, 204
  and 304 responses are returned. To conform with RFC 7230, a Content-Length header field
  is NOT sent for 1xx and 204 responses. Both rules are enforced even if users intentionally
  add body data or the header field for these responses. If violation of these rules is
  detected then an error message is logged. ``RB_ID=917827``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: `$protocol.Client.params/stack` and `$protocol.Server.params/stack` are removed,
  use similar methods on instances instead: `$protocol.client.params/stack` and
  `$protocol.server.params/stack` instead. ``RB_ID=915703``

* finagle-core: Remove deprecated `c.t.f.builder.ClientBuilder.tracerFactory`.
  Use `c.t.f.builder.ClientBuilder.tracer` instead. Remove deprecated
  `c.t.f.tracing.Tracer.Factory`. Use `c.t.f.tracing.Tracer` instead.
  ``RB_ID=915481``

* finagle-core: Remove deprecated `c.t.f.Deadline`. Use `c.t.f.context.Deadline` instead.
  ``RB_ID=915550``

* finagle-core: Remove deprecated `c.t.f.builder.ClientBuilder.cluster` and
  `c.t.f.builder.ClientBuilder.group`. Use `c.t.f.builder.ClientBuilder.dest` instead.
  ``RB_ID=915098``

* finagle-core: Remove deprecated `c.t.f.tracing.Trace.recordRpcName`. Use
  `c.t.f.tracing.Trace.recordRpc` and `c.t.f.tracing.Trace.recordServiceName` instead.
  ``RB_ID=916426``

* finagle-core: Remove deprecated `c.t.f.builder.Cluster`. Use `com.twitter.finagle.Name` to
  represent clusters instead. ``RB_ID=916162``

* finagle-core: LoadBalancerFactory now takes an EndpointFactory which is an
  extension of ServiceFactory that carries an address and has the ability to
  be rebuilt. ``RB_ID=916956``

* finagle-base-http: Remove deprecated `c.t.f.http.Message.ContentTypeWwwFrom`.
  Use `c.t.f.http.Message.ContentTypeWwwForm` instead. ``RB_ID=915543``

* finagle-exception: Remove deprecated `c.t.f.exception.Reporter.clientReporter` and
  `c.t.f.exception.Reporter.sourceReporter`. Use `c.t.f.exception.Reporter.monitorFactory`
  instead. ``RB_ID=916403``

* finagle-http: Remove deprecated `c.t.f.http.HttpMuxer.pattern`. Specify a route
  using `c.t.f.http.HttpMuxer.route(pattern, this)` instead. ``RB_ID=915551``

* finagle-http: Remove deprecated `c.t.f.http.filter.ValidateRequestFilter`. Create a custom
  filter if this behavior is needed. ``RB_ID=915548``

* finagle-kestrel: Remove deprecated methods on `c.t.f.kestrel.MultiReader`:
  - `apply(cluster: Cluster[SocketAddress], queueName: String)`
  - `apply(clients: Seq[Client], queueName: String)`
  - `apply(handles: ju.Iterator[ReadHandle])`
  - `newBuilder(cluster: Cluster[SocketAddress], queueName: String)`
  - `merge(readHandleCluster: Cluster[ReadHandle])`
  Use the `c.t.f.Var[Addr]`-based `apply` methods on `c.t.f.kestrel.MultiReaderMemcache` or `c.t.f.kestrel.MultiReaderThriftMux` instead. ``RB_ID=914910``

* finagle-kestrel: Removed from the project. ``RB_ID=915221``
  https://finagle.github.io/blog/2017/04/06/announce-removals/

* finagle-mdns: Removed from the project. ``RB_ID=915216``
  https://finagle.github.io/blog/2017/04/06/announce-removals/

* finagle-memcached: Remove deprecated `c.t.f.memcached.BaseClient.cas` methods.
  Use `c.t.f.memcached.BaseClient.checkAndSet` instead. ``RB_ID=914678``

* finagle-memcached: `c.t.f.memcached.protocol.text.Encoder` object is now private.
  ``RB_ID=917214``

* finagle-memcached: Make memcached Response subtypes with no fields case objects.
  ``RB_ID=917137``

* finagle-mysql: Remove deprecated methods on `c.t.f.Mysql`:

    - `withCredentials`; use `c.t.f.Mysql.client.withCredentials` instead
    - `withDatabase`; use `c.t.f.Mysql.client.withDatabase` instead
    - `withCharset`; use `c.t.f.Mysql.client.withCharset` instead
    - `configured`; use `c.t.f.Mysql.client.configured` instead

  ``RB_ID=916418``

* finagle-native: Removed from the project. ``RB_ID=915204``
  https://finagle.github.io/blog/2017/04/06/announce-removals/

* finagle-netty4: `AnyToHeapInboundHandler` is gone. Use `BufCodec` while designing
  new Finagle protocols. ``RB_ID=915251``

* finagle-ostrich4: Removed from the project. ``RB_ID=915327``
  https://finagle.github.io/blog/2017/04/06/announce-removals/

* finagle-redis: `ChannelBuffer` methods and converters are removed. Use `Buf`-based API
  instead. Removed APIs: ``RB_ID=916015``

    - `c.t.f.redis.NettyConverters`
    - `c.t.f.redis.util.StringToChannelBuffer`
    - `c.t.f.redis.Client.watch(Seq[ChannelBuffer])`

* finagle-stream: Removed from the project. ``RB_ID=915200``
  https://finagle.github.io/blog/2017/04/06/announce-removals/

* finagle-thrift: Remove deprecated `c.t.f.thrift.transport.netty3.ThriftServerBufferedCodec`
  and `c.t.f.thrift.transport.netty3.ThriftServerBufferedCodecFactory`. Use the `c.t.f.Thrift`
  object to build a server. ``RB_ID=915656``

* finagle-thriftmux: Remove deprecated `c.t.f.ThrifMux.withClientId`. Use
  `c.t.f.ThriftMux.client.withClientId`. Remove deprecated `c.t.f.ThrifMux.withProtocolFactory`.
  Use `c.t.f.ThriftMux.client.withProtocolFactory`. ``RB_ID=915655``

6.44.0
------

New Features
~~~~~~~~~~~~

* finagle-thriftmux: Allow ThriftMux.Servers to be filtered, also add `withStack`
  method to server side as well. ``RB_ID=915095``

* finagle-core: FailureAccrual is now production ready. It has been promoted out of
  experimental and moved from com.twitter.finagle.service.exp to
  com.twitter.finagle.liveness. ``RB_ID=914662``

* finagle-core: SSL/TLS APIs have been changed to include methods which work
  based on an SSL configuration, and an SSL configuration and an SSL engine factory.
  ``RB_ID=911209``

* finagle-core: LoadBalancerFactory now exposes a mechanism to order the collection
  of endpoints passed to the balancer implementations. This allows a consistent ordering
  of endpoints across process boundaries. ``RB_ID=910372``

* finagle-core: Introduce `c.t.f.client.EndpointerStackClient`, a mechanism for
  making clients that don't need a transporter and dispatcher. This simplifies
  making non-netty clients. ``RB_ID=912889``

* finagle-http2: Add support for liveness detection via pings.  It can be configured
  the same way as it is in mux. ``RB_ID=913341``

* finagle-toggle: Standard toggles now track the last value produced from `apply`.
  These values are visible via TwitterServer's /admin/toggles endpoint. ``RB_ID=913925``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-mysql: Support for Netty 3 has been removed, making Netty 4 the only transport
  implementation. ``RB_ID=914661``

* finagle-core: com.twitter.finagle.service.exp.FailureAccrualPolicy has been promoted to
  com.twitter.finagle.liveness.FailureAccrualPolicy

* finagle-commons-stats: Remove finagle-commons-stats, which was a compatibility layer
  for a deprecated stats library.  Please move to finagle-stats instead.  ``RB_ID=910964``

* finagle-core: SSL/TLS stack params for Finagle running Netty 4 have changed.

  - The `TlsConfig` param in `Transport` has been removed.
  - For client engines, the same two parameters as Finagle running Netty 3 are now used:

    - `ClientSsl` in `Transport`, which is used for configuring a client `Engine`'s hostname,
      key credentials, trust credentials, cipher suites, protocols, and application protocols.
    - `SslClientEngineFactory` in `SslClientEngineFactory`, which determines how the `Engine`
      is created based off of an `Address` and an `SslClientConfiguration`.

  - For server engines, the same two parameters as Finagle running Netty 3 are now used:

    - `ServerSsl` in `Transport`, which is used for configuring a server `Engine`'s key
      credentials, trust credentials, cipher suites, protocols, application protocols, and
      where the server supports or requires client authentication.
    - `SslServerEngineFactory` in `SslServerEngineFactory`, which determines how the `Engine`
      is created based off of an `SslServerConfiguration`.

  - Note: Not all client and server configurations work with all engine factories. Each engine
    factory should document what is not supported by that specific engine factory.
  - Note: By default, Finagle on Netty 4 will use the `Netty4ClientEngineFactory` and
    `Netty4ServerEngineFactory` respectively.

  ``RB_ID=910500``

* finagle-core: Change the API to LoadBalancerFactory to a more concrete
  `Activity[IndexedSeq[ServiceFactory[Req, Rep]]]` since the majority of the
  load balancer implementations don't need the properties of a Set but instead
  need ordering guarantees and efficient random access. ``RB_ID=910372``

* finagle-core: Balancers.aperture now has a new parameter `useDeterministicOrdering`,
  which is set to false by default. This feature is still experimental and under
  construction. This will break the Java API and require the additional param to
  be passed in explicitly.  ``RB_ID=911541``

* finagle-core: The logic for tracking sessions that was in StdStackServer has been lifted into
  a new template, ListeningStackServer where implementations define the creation of a
  ListeningServer from a ServiceFactory, SocketAddress, and a function that tracks accepted
  sessions. ``RB_ID=914124``

* finagle-core: Change the AddressOrdering param to no longer take a StatsReceiver,
  since orderings were simplified and are no longer composite. ``RB_ID=914113``

* finagle-core: Remove deprecated methods on `c.t.f.Client`:

    - newClient(dest: Group[SocketAddress])
    - newService(dest: Group[SocketAddress])

  ``RB_ID=914787``

* finagle-core: `c.t.f.ListeningServer` no longer extends `c.t.f.Group`. Use
  `c.t.f.ListeningServer.boundAddress` to extract the address from the server.
  ``RB_ID=914693``

* finagle-core: Remove deprecated `c.t.f.group.StabilizingGroup`. Use
  `c.t.f.addr.StabilizingAddr` instead. ``RB_ID=914823``

* finagle-core: Constructors for `c.t.f.ChannelException` and its subclasses now have
  overloads that take `Option`\s instead of allowing `null`. While the existing
  constructors remain, and forward to the new ones, this can still cause compilation
  failures when the arguments are ambiguous. ``RB_ID=914800``

* finagle-core: Remove MimimumSetCluster since it has been deperecated for quite
  some time. Instead, use finagle logical destinations via `Name`s. ``RB_ID=914849``

* finagle-core: Remove deprecated `c.t.f.Resolver.resolve`. Use `c.t.f.Resolver.bind`
  instead. Remove deprecated `c.t.f.BaseResolver.resolve`. Use `c.t.f.Resolver.eval`
  instead. ``RB_ID=914986``

* finagle-http: `c.t.f.http.Http` codec has disappeared as part of Netty 4 migration. Use
  `c.t.f.Http.client` or `c.t.f.Http.server` stacks instead. ``RB_ID=912427``

* finagle-kestrel: Remove `c.t.f.kestrel.param.KestrelImpl.` Kestrel clients and servers
  now use Netty 4 and cannot be configured for Netty 3. ``RB_ID=911031``

* finagle-memcached: Remove `c.t.f.memcached.param.MemcachedImpl.` Memcached clients and servers
  now use Netty 4 and cannot be configured for Netty 3. ``RB_ID=911031``

* finagle-kestrel: Remove commands that are not supported by the client:

    - `com.twitter.finagle.kestrel.protocol.DumpConfig`
    - `com.twitter.finagle.kestrel.protocol.DumpStats`
    - `com.twitter.finagle.kestrel.protocol.FlushAll`
    - `com.twitter.finagle.kestrel.protocol.Reload`
    - `com.twitter.finagle.kestrel.protocol.ShutDown`
    - `com.twitter.finagle.kestrel.protocol.Stats`
    - `com.twitter.finagle.kestrel.protocol.Version`

  ``RB_ID=911206``

* finagle-memcached: Remove deprecated `c.t.f.memcached.KetamaClientBuilder`. Use
  `c.t.f.Memcached.client` to create a Memcached client. ``RB_ID=907352``

* finagle-memcached: Remove deprecated `c.t.f.memcached.replication.ReplicationClient`. Use
  `c.t.f.memcached.replication.BaseReplicationClient` with clients created using
  `c.t.f.Memcached.client`. ``RB_ID=907352``

* finagle-memcached: Remove deprecated methods on `c.t.f.memcached.Client`:
  - `apply(name: Name)`
  - `apply(host: String)`

  Use `c.t.f.Memcached.client` to create a Memcached client. ``RB_ID=908442``

* finagle-memcached: Remove deprecated `c.t.f.memcached.protocol.text.Memcached` object.
  Use `c.t.f.Memcached.client` to create Memcached clients. ``RB_ID=908442``

* finagle-memcached: Remove deprecated `c.t.f.memcached.Server` class. Use
  `c.t.f.memcached.integration.TestMemcachedServer` for a quick test server.
  ``RB_ID=914827``

* Remove deprecated `c.t.f.memcached.PartitionedClient` object. Use
  `c.t.f.memcached.CacheNodeGroup.apply` instead of
  `c.t.f.memcached.PartitionedClient.parseHostWeights`. ``RB_ID=914827``

* Remove deprecated `c.t.f.memcached.util.ParserUtils.DIGITS`. Use "^\\d+$" instead.
  Remove deprecated `c.t.f.memcached.util.ParserUtils.DigitsPattern`. Use Pattern.compile(^\\d+$)
  instead. ``RB_ID=914827``

* finagle-memcached: Remove old `c.t.f.memcached.replicated.BaseReplicationClient` and
  `c.t.f.memcached.migration.MigrationClient`, and most `c.t.f.memcached.CachePoolCluster`
  methods. ``RB_ID=910986``

* finagle-memcached: Remove old `c.t.f.memcached.migration.DarkRead`, and
  `c.t.f.memcached.migration.DarkWrite`. ``RB_ID=911367``

* finagle-memcached: Remove `c.t.f.memcached.CachePoolConfig`. ``RB_ID=914623``

* finagle-mux: Netty 3 implementation of Mux is removed. Default is
  Netty 4. ``RB_ID=914239``

* finagle-netty4: `DirectToHeapInboundHandler` was renamed to `AnyToHeapInboundHandler`
  and now copies any inbound buffer (not just directs) on heap.  ``RB_ID=913984``

* finagle-thrift, finagle-thriftmux: Remove rich client/server support for prior
  versions of Scrooge generated code. ``RB_ID=911515``

* finagle-core: `c.t.f.client.Transporter` no longer has a close method, which
  was introduced in 6.43.0.  It was sort of a hack, and we saw the opportunity
  to do it properly. ``RB_ID=912889``

* finagle-core, finagle-mux: Move FailureDetector from `c.t.f.mux` to `c.t.f.liveness`.
  This also means that the `sessionFailureDetector` flag is now
  `c.t.f.liveness.sessionFailureDetector`. ``RB_ID=912337``

Bug Fixes
~~~~~~~~~

* finagle-exp: `DarkTrafficFilter` now respects the log level when `HasLogLevel`,
  and otherwise defaults the level to `warning` instead of `error`. ``RB_ID=914805``

* finagle-netty4: Fixed connection stall on unsuccessful proxy handshakes in Finagle clients
  configured with HTTP proxy (`Transporter.HttpProxyTo`).  ``RB_ID=913358``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-netty4: Finagle is no longer logging the failed proxy handshake response.
  ``RB_ID=913358``

* finagle-netty4: SOCKS5 proxies are now bypassed if the connect destination is
  localhost. This matches Finagle's prior behavior from when Netty 3 was the default
  transport implementation. ``RB_ID=914494``

Dependencies
~~~~~~~~~~~~

* finagle-memcached: Remove dependency on com.twitter.common:io-json. ``RB_ID=914623``

6.43.0
------

New Features
~~~~~~~~~~~~

* finagle-base-http: `c.t.f.http.Message` now has a Java friendly method to set the
  HTTP version: `Message.version(Version)`. ``RB_ID=906946``

* finagle-base-http: Added Java friendly methods to the HTTP model including
  `c.t.f.http.Message.contentLength(Long)`, `c.t.f.http.Message.contentLengthOrElse(Long): Long`,
  and `c.t.f.http.Request.method(Method)`. ``RB_ID=907501``

* finagle-base-http: `c.t.f.http.HeaderMap` now has a method, `HeaderMap.newHeaderMap` for
  creating new empty `HeaderMap` instances. ``RB_ID=907397``

* finagle-core: SSL/TLS client and server configurations and engine factories have
  been added for finer grained control when using TLS with Finagle. ``RB_ID=907191``

* finagle-netty4: Introducing a new toggle `com.twitter.finagle.netty4.UsePooling` that
  enables byte buffers pooling in Netty 4 pipelines. ``RB_ID=912789``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-base-http: `c.t.f.http.MapHeaderMap` has been made private. Please use
  `HeaderMap.apply` or `HeaderMap.newHeaderMap` to construct a new `HeaderMap` instance.
  ``RB_ID=907397``

* finagle-base-http: `c.t.f.http.Version` is no longer represented by case objects
  and has been replaced by val instances of a case class. ``RB_ID=906946``

* finagle-base-http: The common HTTP methods are no longer modeled by case objects but
  as instances of a single c.t.f.http.Method class. The string representation of the HTTP
  method is now available via the `Method.name` method. ``RB_ID=906697``

* finagle-core: Move the `java.net.SocketAddress` argument from the `apply` method
  on `com.twitter.finagle.client.Transporter` to the `newTransporter` method of
  `com.twitter.finagle.client.StackClient`. ``RB_ID=907544``

* finagle-core: Load Balancer implementations no longer mix-in the OnReady trait and
  OnReady was removed. ``RB_ID=908863``

* finagle-core: HeapBalancer, ApertureLoadBalancer, and RoundRobinBalancer classes were
  made package private. To construct load balancers for use within a Finagle client,
  use the `com.twitter.finagle.loadbalancer.Balancers` object. ``RB_ID=909245``

* finagle-core: The `aperture` constructor on the `Balancers` object no longer takes
  a Timer since it was unused. ``RB_ID=909245``

* finagle-core: The load balancer algorithm is now further scoped under "algorithm".
  ``RB_ID=909309``

* finagle-core: Remove `Ring` from Finagle core's util since it is unused
  internally. ``RB_ID=909718``

* finagle-core: SSL/TLS stack params for Finagle running Netty 3 have changed.

    - The `TLSClientEngine` param in `Transport` has been replaced by two parameters:

      - `ClientSsl` in `Transport`, which is used for configuring a client `Engine`'s hostname,
        key credentials, trust credentials, cipher suites, protocols, and application protocols.
      - `SslClientEngineFactory` in `SslClientEngineFactory`, which determines how the `Engine`
        is created based off of an `Address` and an `SslClientConfiguration`.

    - The `TLSHostname` param in `Transporter` has been removed. Hostnames should be set as
      part of the `SslClientConfiguration` now.
    - The `TLSServerEngine` param in `Transport` has been replaced by two parameters:

      - `ServerSsl` in `Transport`, which is used for configuring a server `Engine`'s key
        credentials, trust credentials, cipher suites, protocols, application protocols, and
        whether the server supports or requires client authentication.
      - `SslServerEngineFactory` in `SslServerEngineFactory`, which determines how the `Engine`
        is created based off of an `SslServerConfiguration`.

    - Note: Not all client and server configurations work with all engine factories. Each engine
      factory should document what is not supported by that specific engine factory.
    - Note: Users using Finagle-Native should in the short term use `LegacyServerEngineFactory`
      and in the long term move to using `Netty4ServerEngineFactory`.
    - Note: With this change, private keys are expected to explicitly be PKCS#8 PEM-encoded keys.
      Users using PKCS#1 keys should in the short term use `LegacyKeyServerEngineFactory` and in
      the longer term switch to using PKCS#8 keys, or use your own `SslServerEngineFactory` which
      can explicitly handle those type of keys.
    - Note: By default, Finagle on Netty 3 will use the `JdkClientEngineFactory` and
      `JdkServerEngineFactory` respectively.

    ``RB_ID=907923``

* finagle-core: `withLoadBalancer.connectionsPerEndpoint` was removed and moved
  into finagle-memcached, which was the only client that uses the feature. ``RB_ID=908354``

* finagle-core: `ClientBuilder.expHttpProxy` and `ClientBuilder.expSocksProxy` are removed.
  Use `$Protocol.withTransport.httpProxyTo` instead (requires Netty 4 transport). ``RB_ID=909739``

* finagle-kestrel: Remove the deprecated `codec` method on `c.t.f.kestrel.MultiReaderMemcache`.
  Use `.stack(Kestrel.client)` on the configured `c.t.f.builder.ClientBuilder` instead.
  ``RB_ID=907184``

* finagle-kestrel: Removed `c.t.f.kestrel.Server`. A local Kestrel server is preferred for
  testing. ``RB_ID=907334``

* finagle-kestrel: Removed deprecated `c.t.f.kestrel.protocol.Kestrel`. To create a Finagle
  Kestrel client, use `c.t.f.Kestrel.client`. ``RB_ID=907422``

* finagle-serversets: Removed the unapply method and modified the signature of
  fromAddrMetadata method in `c.t.f.serverset2.addr.ZkMetadata`. Instead of pattern
  matching use the modified fromAddrMetadata method. ``RB_ID=908186``

* finagle-stats: Remove the `com.twitter.finagle.stats.exportEmptyHistograms` toggle
  which has defaulted to 0.0 for quite some time. Change the default value of the
  `com.twitter.finagle.stats.includeEmptyHistograms` flag to false to retain the
  behavior. ``RB_ID=907186``

* finagle-thrift: `ThriftServiceIface` was refactored to be in terms of `ThriftMethod.Args`
  to `ThriftMethod.SuccessType` instead of `ThriftMethod.Args` to `ThriftMethod.Result`.
  ``RB_ID=908846``

* finagle-redis: Remove pendingCommands from `c.t.f.finagle.redis.SentinelClient.Node` and
  add linkPendingCommands for compatibility with redis 3.2 and newer.
  ``RB_ID=913516``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: Responses with a server error status code (500s) are now classified
  as a failure. This effects success rate metrics and failure accrual.
  See the `com.twitter.finagle.http.serverErrorsAsFailuresV2` toggle for opting
  out of this behavior. ``RB_ID=909315``

* finagle-netty4: Servers no longer set SO_LINGER=0 on sockets. ``RB_ID=907325``

Deprecations
~~~~~~~~~~~~

* finagle-base-http: The `c.t.f.http.Response` methods `getStatusCode()` and `setStatusCode()`
  have been deprecated. Use the methods `statusCode` and `statusCode(Int)` instead.
  ``RB_ID=908409``

* finagle-core: `c.t.f.builder.ClientBuilder.group` and `c.t.f.builder.ClientBuilder.cluster`
  have been deprecated. Use `c.t.f.builder.ClientBuilder.dest` with a `c.t.f.Name` instead.
  ``RB_ID=914879``

* finagle-http: Now that `c.t.f.http.Method` and `c.t.f.http.Version` are represented by
  instances and thus easier to use from Java, the Java helpers `c.t.f.http.Versions`,
  `c.t.f.http.Statuses`, and `c.t.f.http.Methods` have been deprecated. ``RB_ID=907680``

* finagle-memcached: `c.t.f.memcached.replication.ReplicationClient` is now deprecated. Use
  `c.t.f.memcached.replication.BaseReplicationClient` with clients created using
  `c.t.f.Memcached.client`. ``RB_ID=907384``

* finagle-thrift: As part of the Netty 4 migration, all `c.t.f.Codec` and `c.t.f.CodecFactory`
  types in finagle-thrift are now deprecated. Use the `c.t.f.Thrift` object to make clients
  and servers. ``RB_ID=907626``

Bug Fixes
~~~~~~~~~

* finagle-core: Fix `ConcurrentModificationException` thrown by calling `close()` on
  `c.t.f.factory.ServiceFactoryCache`. ``RB_ID=910407``

* finagle-http: The HTTP/1.x Client will no longer force-close the socket after receiving
  a response that lacks content-length and transfer-encoding headers but is required per
  RFC 7230 to not have a body. ``RB_ID=908593``

* finagle-redis: The HSCAN and SCAN commands take an optional argument for pattern matching.
  This argument has been fixed to use the correct name of 'MATCH' instead of the incorrect
  'PATTERN'. ``RB_ID=908817``

* finagle-thrift: Properly locate sub-classed MethodIface services to instantiate for serving
  BaseServiceIface implemented thrift services. ``RB_ID=907608``

* finagle-redis: The SentinelClient will no longer throw an NoSuchElementException when
  initializing connections to a redis 3.2 or greater sentinel server. ``RB_ID=913516``

Dependencies
~~~~~~~~~~~~

* finagle: Bump guava to 19.0. ``RB_ID=907807``

6.42.0
------

New Features
~~~~~~~~~~~~

* finagle-commons-stats: Provide a TwitterServer exporter for commons stats.
  This simplifies migration for folks who don't want to switch to
  commons metrics and TwitterServer in one go.  It will export stats on the
  /vars.json endpoint.  ``RB_ID=902921``

* finagle-http: Introduce `HeaderMap.getOrNull(header)`, a Java-friendly variant of
  `HeaderMap.get(header).orNull`.  ``RB_ID=904093``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: finagle-http-compat has been removed as part of migration off Netty 3. Use
  finagle-http types/APIs directly. ``RB_ID=903647``

* finagle: finagle-spdy has been removed as part of the migration off Netty 3. Please
  use finagle-http2 as a replacement. ``RB_ID=906033``

* finagle-base-http: `Message.write(ChannelBuffer)` has been replaced with a method that
  receives a `Buf`. The semantics of calling the `write` method on chunked messages has
  changed from potentially throwing an exception based on the state of the `Writer` to
  always throwing an `IllegalStateException`. Existing users of the `write(..)` methods
  on chunked messages should use the `Writer` directly. ``RB_ID=900091``

* fingle-base-http: `HeaderMap.getAll(key)` now returns a `Seq[String]` as opposed to a
  `Iterable[String]`. ``RB_ID=905019``

* finagle-core: The ChannelTransport implementations which transforms a Netty pipeline into
  a finagle Transport[Req, Rep] have been specialized to Transport[Any, Any] to avoid the
  illusion of a runtime checked cast. Transport.cast has been changed to receive either a
  Class[T] or an implicit Manifest[T] in order to check the inbound cast at runtime. For users
  of the ChannelTransport types, use the Transport.cast method to get a Transport of the right
  type. ``RB_ID=902053``

* finagle-memcached: Remove deprecated methods on `c.t.f.memcached.Client`:
    - `apply(group: Group[SocketAddress])`
    - `apply(cluster: Cluster[SocketAddress])`

  Use `c.t.f.Memcached.client` to create a Memcached client. ``RB_ID=899331``

* finagle-toggle: `ToggleMap` `Toggles` now rehash the inputs to
  `apply` and `isDefinedAt` in order to promote a relatively even
  distribution even when the inputs do not have a good distribution.
  This allows users to get away with using a poor hashing function
  such as `String.hashCode`. ``RB_ID=899195``

Deprecations
~~~~~~~~~~~~

* finagle-base-http: Deprecate `c.t.f.http.MapHeaderMap` as it will
  soon be private. Use `c.t.f.http.HeaderMap.apply(..)` to get a HeaderMap
  instance. ``RB_ID=906497``

* finagle-base-http: Deprecate `c.t.f.http.HeaderMap += (String, Date)`.
  Use `c.t.f.http.HeaderMap.set(String, Date)` instead. ``RB_ID=906497``

* finagle-base-http: Deprecate `c.t.f.http.Message.ContentTypeWwwFrom`.
  Use `c.t.f.http.Message.ContentTypeWwwForm` instead. ``RB_ID=901041``

* finagle-base-http: Deprecate `c.t.f.http.Message.headers()`. Use
  `c.t.f.http.Message.headerMap` instead. ``RB_ID=905019``

* finagle-base-http: Deprecate the lazy `response: Response` field on the Request type.
  This field is potentially hazardous as it's not necessarily the Response that will
  be returned by a Service but it is often used as such. Construct a Response using
  the static constructor methods. ``RB_ID=899983``

* finagle-base-http: Numerous protected[finagle] methods on `http.Request` and
  `http.Response` that deal in Netty 3 types have been deprecated as part of the
  migration to Netty 4. ``RB_ID=905761``

* finagle-http: Deprecate ValidateRequestFilter which now has limited utility.
  See entry in Runtime Behavior Changes. If this is still needed, copy the remaining
  behavior into a new filter. ``RB_ID=899895``

* finagle-memcached: Deprecate methods on `c.t.f.memcached.Client`:
    - `apply(name: Name)`
    - `apply(host: String)`

  Use `c.t.f.Memcached.client` to create a Memcached client. `RB_ID=899331``

* finagle-memcached: Deprecate `c.t.f.memcached.protocol.text.Memcached` object.
  Use `c.t.f.Memcached.client` to create Memcached clients. ``RB_ID=899009``

* finagle-memcached: Deprecations on `c.t.f.memcached.util.ParserUtils`:
    - For `isDigits(ChannelBuffer)` use `ParserUtils.isDigits(Buf)` instead.
    - `DIGITS`
    - `DigitsPattern`

  ``RB_ID=905253``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: The HTTP client will no longer emit a Netty 3/4 `TooLongFrameException` when
  a response exceeds the specified MaxResponseSize parameter, and instead emits a Finagle
  specific `TooLongMessageException` which wraps the Netty exception. ``RB_ID=905567``

* finagle-http: ValidateRequestFilter doesn't look for the uri "/bad-http-request" which
  had been indicative of the netty3 http codec giving up on decoding a http request. These
  events are caught lower in the pipeline and should not bubble up to the level of this
  filter. ``RB_ID=899895``

* finagle-netty4: DirectToHeapHandler is now aware of `ByteBufHolder` types hence can copy
  them on to heap. ``RB_ID=906602``

* finagle-redis: Transport implementation is now based on Netty 4 (instead of Netty 3).
  ``RB_ID=895728``

Bug Fixes
~~~~~~~~~

* finagle-core: Properly compute length when converting a `Buf.ByteArray`
  to a Netty 4 `ByteBuf`. ``RB_ID=901605``

* finagle-memcached: AtomicMap change lock function to synchronize on map
  object. ``DIFF_ID=D18735``

* finagle-netty4: Fixed connection stall in Finagle clients configured with
  both HTTP proxy (`Transporter.HttpProxyTo`) and TLS/SSL enabled. ``RB_ID=904831``

* finagle-netty4: Fixed connection stall in Finagle clients configured with
  both HTTP proxy (`Transporter.HttpProxy`) and TLS/SSL enabled. ``RB_ID=904803``

6.41.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Added stat "pending_requests/rejected" for the number of requests
  rejected by `c.t.f.PendingRequestFilter`. ``RB_ID=898184``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core:  Remove the `Filter#andThen(Req1 => Future[Rep1]): Req2 => Future[Rep2]`
  method.  This overload is no longer usable in scala 2.12, because `Service`
  is a SAM.  Because of the order in which SAMs get resolved, literal
  functions in scala will get confused about which method they should use.
  Instead of passing a Function directly, wrap the Function with a Service.mk.
  ``RB_ID=896524``

* finagle-core: `CancelledWriteException` was removed as it is no longer used.
  ``RB_ID=896757``

* finagle-core: The structure of Context and its subtypes, LocalContext and
  MarshalledContext, have been significantly refined, eliminating StackOverflowErrors
  and memory leaks while also refining the API. The `letClear()` method, which cleared all
  items from the context, has been renamed to `letClearAll` to avoid confusion with other
  `letClear` methods which clear individual keys. The bulk `letClear` method now takes
  a collection of `Key[_]`s, making it usable from Java. Bulk `let` operations can now be
  done using a collection of `KeyValuePair`s. ``RB_ID=896663``

* finagle-kestrel: The `codec` method has been removed from the kestrel
  MultiReader object. Configure a ClientBuilder protocol using the default
  thrift StackClient, Thrift.client, via the `stack` method of ClientBuilder.
  ``RB_ID=894297``

* finagle-memcached: Remove deprecated `cluster` method on `c.t.f.memcached.KetamaClientBuilder`.
  ``RB_ID=898365``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.builder.ClientBuilder` remove deprecated methods.
  The same functionality is available through the Stack-based APIs or
  `ClientBuilder.configured`, with the exception of `channelFactory`, which
  has no analog because it exposes a Netty 3 API. ``RB_ID=893147``

    - `channelFactory`
    - `expHostConnectionBufferSize`
    - `hostConnectionIdleTime`
    - `hostConnectionMaxIdleTime`
    - `hostConnectionMaxLifeTime`
    - `hostConnectionMaxWaiters`
    - `readerIdleTimeout`
    - `recvBufferSize`
    - `sendBufferSize`
    - `writerIdleTimeout`

* finagle-core: Lower logging level used in `c.t.f.util.DefaultMonitor` for expected
  exceptions: `CancelledRequestException`, `TooManyWaitersException`,
  `CancelledConnectionException`, `FailedFastException`. ``RB_ID=895702``

* finagle-core: `c.t.f.util.DefaultMonitor` now logs most exceptions at
  `WARNING` level instead of `FATAL`. ``RB_ID=895983``

* finagle-core: `c.t.f.util.DefaultMonitor` works harder to find the appropriate
  log level by walking the exception's causes to find `c.t.util.TimeoutExceptions`
  and `c.t.logging.HasLogLevel`. ``RB_ID=896695``

* finagle-core: The `c.t.f.service.Retries` module will now flag a response as
  `NonRetryable` if either the retry limit is reached or the retry budget is exhausted.
  ``RB_ID=897800``

* finagle-mdns: Uses only one implementation backed by jmdns instead of trying
  to use a platform specific implementation of DDNS if present. ``RB_ID=897917``

* finagle-netty4: Client initiated TLS/SSL session renegotiations are now rejected
  by default. ``RB_ID=895871``

* finagle-netty4: `ChannelTransport` no longer interrupts netty write operations
  in order to temporarily unblock rollout of netty4. This reverts netty4 back
  to netty3 semantics for writes. ``RB_ID=896757``

Deprecations
~~~~~~~~~~~~

* finagle-kestrel: Deprecate the `codec` method on `c.t.f.kestrel.MultiReaderMemcache`.
  Use `.stack(Kestrel.client)` on the configured `c.t.f.builder.ClientBuilder` instead.
  ``RB_ID=895989``

6.40.0
------

New Features
~~~~~~~~~~~~

* finagle: Most libraries (excluding finagle-thrift{,mux}) no longer need to
  add an additional resolver that points to maven.twttr.com. ``RB_ID=878967``

* finagle: Introducing a new Finagle module `finagle-base-http` that provides
  a common ground for both Netty 3 (`finagle-http`) and Netty 4 (`finagle-netty4-http`)
  HTTP implementations. Netty 3 is still a default transport used in Finagle's
  `Http.client` and `Http.server`. ```RB_ID=884614``

* finagle-core: Introduce the `c.t.f.client.DynamicTimeout` module allowing clients
  to specify call-site specific timeouts. ``RB_ID=885005``

* finagle-core: A new module, `c.t.f.service.DeadlineFilter`, can be added to stack-based servers
  and clients, which rejects requests with expired deadlines ``RB_ID=895820``

* finagle-memcached: Introduce `c.t.f.memcached.CasResult.replaced: Boolean`
  to help transition usage off of the deprecated `cas` client method to
  `checkAndSet`. ``RB_ID=891628``

* finagle-thrift: We now depend on a fork of libthrift hosted in the Central Repository.
  The new package lives in the 'com.twitter' organization. This removes the necessity of
  depending on maven.twttr.com. This also means that eviction will not be automatic and
  using a newer libthrift library requires manual eviction if artifacts are being pulled
  in transitively. ``RB_ID=885879``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `Monitor` logging is improved in Finagle. ``RB_ID=878890``

  - All exceptions caught in the stack are now logged by Finagle's `DefaultMonitor`
    (previously Util's `RootMonitor`) such that Twitter's logging
    framework is used instead of JDK logging.

  - `DefaultMonitor` is now installed implicitly such that it will be composed
    (via `orElse`) with the monitor passed by a user through the stack param.
    The logic behind this compostion is quite straightforward: exceptions that
    are't handled by a user-defined monitor propagated to the default monitor.

  - `DefaultMonitor` now logs upstream socket address, downstream socket address,
    and a client/server label if those are available.

  - `RootMonitor` is still used to handle fatal exceptions from pending side-effect-only
    closures (i.e., `onFailure`, `onSuccess`) on a service future/promise.

* finagle-core: `c.t.f.service.DeadlineStatsFilter` has been removed from the
  server stack, along with all related stats.
  The "admission_control/deadline/transit_latency_ms" stat has been moved to
  `c.t.f.filter.ServerStatsFilter` and re-scoped as "transit_latency_ms"
  ``RB_ID=895820``

* finagle-mux: `com.twitter.finagle.Failures` are now sent over the wire with
  their flags intact via `com.twitter.finagle.mux.transport.MuxFailure` in the
  previously unused Rdispatch context. This allows for greater signaling along
  a chain of services. See the "MuxFailure Flags" section of the mux protocol
  spec in finagle-mux/src/main/scala/c/t/f/mux/package.scala ``RB_ID=882431``

* finagle-netty4: The netty4 listener + transporter no longer manage direct byte
  buffers by default. c.t.f.netty4.channel.DirectToHeapInboundHandler is introduced
  to help protocol builders manage them. ``RB_ID=881648``

* finagle-stats: Changed the default behavior of empty histograms to only export
  the count. Thus the `com.twitter.finagle.stats.exportEmptyHistograms` toggle
  now defaults to `0.0`. ``RB_ID=882522``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: Some APIs around configuring TLS/SSL on Finagle clients have changed to
  unblock Netty 4 adoption. ``RB_ID=890935``

  - `c.t.f.Http.client.withTls(Netty3TransporterTLSConfig)` is removed. Use
    variations of `c.t.f.Http.client.withTransport.tls` instead.

  - `c.t.f.netty3.Netty3TransporterTLSConfig` is removed.

* finagle: Some APIs around configuring TLS/SSL on Finagle servers have changed to
  unblock Netty 4 adoption. ``RB_ID=891270``

  - `c.t.f.Http.server.withTls(Netty3ListenerTLSConfig)` is removed. Use
    variations of `c.t.f.Http.server.withTransport.tls` instead.

  - `c.t.f.netty3.Netty3ListenerTLSConfig` is removed.

* finagle-core: Removed the protected and unused method `read(permit: Permit): Future[Rep]`
  from `SerialClientDispatcher`. ``RB_ID=881978``

* finagle-core: Removed a gauge, `idle`, from `c.t.f.factory.ServiceFactoryCache`.
  ``RB_ID=884210``

* finagle-core: `ServiceTimeoutException` now extends `NoStackTrace`. ``RB_ID=886809``

* finagle-core: Marked `com.twitter.finagle.util.ConcurrentRingBuffer` as
  private.  It doesn't fit the typical programming model we encourage for
  users of finagle, and so we found it was rarely used.  ``RB_ID=888801``

* finagle-core: Marked `transform` in `com.twitter.finagle.Stack` as protected. It is
  too powerful and unnecessary for users, and should be used by implementors only.

* finagle-core: Removed the `StatsReceiver` argument from `TimeoutFilter`.  ``RB_ID=891380``

* finagle-core: Stopped exporting a few metrics related to deadlines, and replaced with a simpler
  one.  There was a per-`TimeoutFilter` one named `timeout/expired_deadline_ms`, and a per-server
  one named `admission_control/deadline/deadline_budget_ms`.  We added instead a per-server one
  named `admission_control/deadline/exceeded_ms`. ``RB_ID=891380``

* finagle-http: HttpMuxer now takes in a Seq[Route] instead of a
  Seq[(String, Service[Request, Response])]. ``RB_ID=886829``

* finagle-http: As part of the first step towards restructuring Finagle HTTP modules
  required for Netty 4 adoption, HTTP params are moved from the inner object `c.t.f.Http.param`
  into their own package `c.t.f.http.param`. ``RB_ID=885155``

* finagle-redis: A couple of methods had to be renamed (and return type changed) to
  unblock Netty 4 adoption. ``RB_ID=882622``

  - `Command.toChannelBuffer` renamed to `Command.toBuf` and return
    type changed from N3 `ChannelBuffer` to Finagle `Buf`.

  - `Command.toByteArray` is removed.

  - Both `Command.key` and `Command.value` now implemented in terms of `Buf`s
    (no `ChannelBuffers`).

* finagle-redis: An API around `c.t.f.redis.protocol.Command` was modernized as part of
  major restructuring required for the Netty 4 adoption. ``RB_ID=885811``

  - `RedisMessage` (a common parent for both `Command` and `Reply`) has been removed.

  - The encoding machinery was restructured to eliminate duplicated and dead code.

* finagle-thrift: Removed deprecated `ThriftRichClient.newServiceIface` methods
  which did not take a label. Use the versions that take a String label.
  ``RB_ID=891004``

* finagle-thrift: Removed deprecated `ThriftRichClient.newIface` methods based
  on `Groups`. Use the versions that a `dest` or `Name`. ``RB_ID=891004``

* finagle-thriftmux: Removed deprecated classes `ThriftMuxClient`, `ThriftMuxClientLike`,
  `ThriftMuxServer`, and `ThriftMuxServerLike`. ``RB_ID=880924``

Bug Fixes
~~~~~~~~~

* finagle-core: The `withTlsWithoutValidation` and `tlsWithoutValidation`
  APIs have been fixed for an issue on Java 8 where certificate validation
  was being attempted instead of bypassed. ``RB_ID=881660``

* finagle-http: The toggle implementation for `com.twitter.finagle.http.serverErrorsAsFailures`
  had a bug when toggled on. That toggle is no longer used and is superseded by
  `com.twitter.finagle.http.serverErrorsAsFailuresV2`. ``RB_ID=882151``

* finagle-netty4: Connecting to a Socks 5 proxy using Finagle with Netty 4
  now works properly. This previously resulted in a timeout and
  `ProxyConnectException`. ``RB_ID=884344``

* finagle-netty4: Don't swallow bind failures. ``RB_ID=892217``

Deprecations
~~~~~~~~~~~~

* finagle-core: `c.t.f.builder.ClientBuilder` deprecate some seldom used methods.
  The same functionality is available through the Stack-based APIs or
  `ClientBuilder.configured`. ``RB_ID=881612``

   - `hostConnectionIdleTime`
   - `hostConnectionMaxIdleTime`
   - `hostConnectionMaxLifeTime`
   - `hostConnectionMaxWaiters`

6.39.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `com.twitter.finagle.Failure` has a new flag, `NonRetryable`,
  which signifies that a request should not be retried. The flag is respected by
  all of Finagle's retry mechanisms. ``RB_ID=878766``

* finagle-thriftmux: Allow ThriftMux.Clients to be filtered. This is supported
  in the the StdStackClient but ThriftMux.Client is a StackBasedClient. ``RB_ID=874560``

* finagle-netty4: Add boolean flag `com.twitter.finagle.netty4.poolReceiveBuffers` that
  enables/disables pooling of receive buffers (disabled by default). When enabled, lowers
  the CPU usage and allocation rate (GC pressure) with the cost of increased memory
  footprint at the startup. ``RB_ID=872940``

* finagle-netty4: Add new histogram `receive_buffer_bytes` (only enabled with pooling)
  to keep track of the receive buffer sizes (useful for tuning pooling). ``RB_ID=877080``

Deprecations
~~~~~~~~~~~~

* finagle-core: `c.t.f.builder.ClientBuilder` deprecate some seldom used methods.
  The same functionality is available through the Stack-based APIs or
  `ClientBuilder.configured`. ``RB_ID=878009``

  - `readerIdleTimeout`
  - `writerIdleTimeout`
  - `recvBufferSize`
  - `sendBufferSize`
  - `channelFactory`
  - `expHostConnectionBufferSize`

* finagle-kestrel: Deprecate `c.t.f.kestrel.protocol.Kestrel()`,
  `c.t.f.kestrel.protocol.Kestrel(failFast)`, and `c.t.f.kestrel.protocol.Kestrel.get()`.
  To create a Kestrel client using ClientBuilder, use `.stack(c.t.f.Kestrel.client)`.
  ``RB_ID=870686``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: The constructors for both `c.t.f.netty3.Netty3Listener` and
  `c.t.f.netty3.Netty3Transporter` now take `Stack.Params` instead of
  individual parameters. ``RB_ID=871251``

* finagle-thrift: The c.t.f.thrift.legacy package has been removed which included
  the public types `ThriftCall`, `ThriftReply`, and `ThriftCallFactory`.
  ``RB_ID=873982``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Tolerate TraceIds that are greater than 64 bits in preparation of
  moving to 128 bit ids. ``RB_ID=874365``

* finagle-http: `c.t.f.http.service.HttpResponseClassifier.ServerErrorsAsFailures` now
  classifies a retryable nack as a `ResponseClass.RetryableFailure`. ``RB_ID=869182``

* finagle-http: `c.t.f.Http.{Client,Server}` is moving towards treating HTTP
  5xx status codes as failures via
  `c.t.f.http.service.HttpResponseClassifier.ServerErrorsAsFailures`. This can
  be disabled by setting the toggle "com.twitter.finagle.http.serverErrorsAsFailures"
  to `0.0` or explicitly setting it using `withResponseClassifier`.
  ``RB_ID=869303``, ``RB_ID=875367``

* finagle-http: `c.t.f.http.HttpServerDispatcher` now removes the bodies from responses
  to HEAD requests in order to avoid a HTTP protocol error and logs the event at level
  error. Associated with this, the `c.t.f.http.filter.HeadFilter` will now strip the body
  from a chunked response, but in these cases the `Writer` associated with the response
  will receive a `ReaderDiscarded` exception if a write is attempted after the filter has
  run. ``RB_ID=872106``

* finagle-thrift: Also track response failures in the
  `c.t.finagle.thrift.ThriftServiceIface#statsFilter` in addition to successful
  responses that encode an Error or Exception. ``RB_ID=879075``

Bug Fixes
~~~~~~~~~

* finagle-http: Fix issue in `c.t.finagle.http.RequestBuilder` when the URI host contains
  underscores. ``RB_ID=870978``

* finagle-http: A connection for HTTP/1.0 or non-keep-alive requests is now closed
  gracefully so that all requests that have been issued received responses.
  ``RB_ID=868767``

* finagle-http: The HTTP/1.x server dispatcher no longer clobbers 'Connection: close'
  headers set by a service, resulting in the graceful shutdown of the connection.
  ``RB_ID=880007``

* finagle-netty4: `ChannelTransport` now drains messages before reading more data off the
  transport which should reduce memory pressure in streaming protocols. ``RB_ID=872639``

6.38.0
------

New Features
~~~~~~~~~~~~

* finagle-http: `HttpNackFilter` now handles both retryable and non-retryable nacks via a new
  header: "finagle-http-nonretryable-nack". These are converted to non-retryable `c.t.f.Failures`
  and counted by a new counter "nonretryable_nacks". ``RB_ID=865468``

* finagle-toggle: Is no longer considered experimental. ``RB_ID=868819``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-kestrel: `c.t.f.kestrel.protocol.ResponseToEncoding` is now private[finagle].
  ``RB_ID=866612``

* finagle-memcached: `c.t.f.memcached.protocol.text.{CommandToEncoding, ResponseToEncoding}`,
  `c.t.f.memcached.protocol.text.client.{AbstractDecodingToResponse, ClientTransport, DecodingToResponse}` are now private[finagle]. ``RB_ID=866612``

* finagle-netty4: Move `numWorkers` flag out of the package object so it gets a
  user friendly name: `c.t.f.netty4.numWorkers` instead of `c.t.f.netty4$.package$.numWorkers`.
  ``RB_ID=123567``

* finagle-core: `c.t.f.netty3.WorkerPool` is no longer visible outside of `c.t.f.netty3`.
  ``RB_ID=123567``

* finagle-core: The `content` parameter of the `ClientSslContext` and
  `ClientSslContextAndHostname` `TlsConfig` options has been renamed to `context`.
  ``RB_ID=868791``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-thriftmux: Removed "<server_label>/thriftmux/downgraded_connections" and
  "<server_label>/thriftmux/connections" gauges. Counters are still available at
  "<server_label>/thrifmux/connects" and "<server_label>thriftmux/downgraded_connects".
  ``RB_ID=867459``

6.37.0
------

Deprecations
~~~~~~~~~~~~

* finagle-core: `c.t.f.Deadline` is deprecated in favor of `c.t.f.context.Deadline`.
  ``RB_ID=864148``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: As part of a move away from encoding/decoding in the Netty pipeline, removed
  `FrameEncoder` and `FrameDecoder` types, found in `c.t.f.codec`. ``RB_ID=847716``

* finagle-core: Delete IdleConnectionFilter, which is no longer hooked up in the server, and
  no longer seems to be useful.  ``RB_ID=856377``

* finagle-core: Remove deprecated methods from `c.t.f.builder.ClientBuilder` ``RB_ID=864622``

    - `connectionTimeout`, use `tcpConnectTimeout`
    - `expFailFast`, use `failFast`
    - `buildFactory`, use other `buildFactory` methods
    - `build`, use other `build` methods

* finagle-exp: Abstract out parts of the DarkTrafficFilter for potential re-use.
  We also canonicalize the DarkTrafficFilter stats scope which changes from
  "darkTrafficFilter" to "dark_traffic_filter". E.g.:
  "dark_traffic_filter/forwarded", "dark_traffic_filter/skipped", and
  "dark_traffic_filter/failed". ``RB_ID=852548``

* finagle-mysql: Mysql has been promoted out of experimental. Please change all
  references of com.twitter.finagle.exp.{M,m}ysql to com.twitter.finagle.{M,m}ysql

* finagle-redis: Server-side support for Redis is removed. See this finaglers@ thread
  (https://groups.google.com/forum/#!topic/finaglers/dCyt60TJ7eM) for discussion.
  Note that constructors for Redis commands no longer accept raw byte arrays.
  ``RB_ID=848815``

* finagle-redis: Redis codec (i.e., `c.t.f.Codec`) is removed. Use `c.t.f.Redis.client`
  instead. ``RB_ID=848815``

New Features
~~~~~~~~~~~~

* finagle-core: Expose metrics on util's default `FuturePool` implementations
  `unboundedPool` and `interruptibleUnboundedPool`:
  "finagle/future_pool/pool_size", "finagle/future_pool/queue_size",
  "finagle/future_pool/active_tasks", and "finagle/future_pool/completed_tasks".
  ``RB_ID=850652``

* finagle-core: Mux Clients now propagate the number of times the client retried
  the request in the request's c.t.f.context.Context, available via
  c.t.f.context.Retries. ``RB_ID=862640``

* finagle-http: HTTP Clients now propagate the number of times the client retried
  the request in the request's c.t.f.context.Context, available via
  c.t.f.context.Retries. ``RB_ID=864852``

* finagle-thrift: maxThriftBufferSize is now tunable via parameter for Thrift
  servers. It previously only was for ThriftMux servers. ``RB_ID=860102``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-http: HttpTransport now eagerly closes client connection after
  processing non-keepalive requests.

* finagle-redis: `c.t.f.redis.Client` now uses the pipelining dispatcher. ``RB_ID=848815``

* finagle-serversets: `c.t.f.serverset2.Stabilizer` no longer uses a timer to implement
  stabilization periods if the periods are 0 seconds long. ``RB_ID=861561``

* finagle-core: 'c.t.f.Failure' has a new flag, Rejected, to indicate that a given request was
  rejected. All Failures generated with the Failure.rejected constructor are flagged Rejected and
  Restartable. ``RB_ID=863356``

* finagle-core: `c.t.f.FixedInetResolver` now optionally retries failed DNS
  lookups with provided backoff, and `c.t.f.serverset2.Zk2Resolver` uses this
  retry functionality infinitely, exponentially backing off from 1 second to
  5 minutes. ``RB_ID=860058``

6.36.0
------

Deprecations
~~~~~~~~~~~~

* finagle-http: Removed DtabFilter.Finagle in favor of DtabFilter.Extractor.
  ``RB_ID=840600``

* finagle-zipkin: Deprecate `ZipkinTracer` in favor of `ScribeZipkinTracer`.
  ``RB_ID=840494``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle: Builds are now only for Java 8 and Scala 2.11. See the
  `blog post <https://finagle.github.io/blog/2016/04/20/scala-210-and-java7/>`_
  for details. ``RB_ID=828898``

* finagle: Finagle is no longer depending on Twitter's clone of JSR166e, JDK 8
  API is used instead. ``RB_ID=833652``

* finagle-cacheresolver: package contents merged into finagle-memcached.
  ``RB_ID=833602``

* finagle-core: Renamed DeadlineFilter to DeadlineStatsFilter, which now only
  records stats for the number of requests with exceeded deadlines, the
  remaining deadline budget, and the transit latency of requests. It no longer
  rejects requests and has no configuration. We have decided not to pursue
  Deadline Admission Control at this time. ``RB_ID=829372``

* finagle-core: `ClientBuilder.socksProxy(SocketAddress)` is removed.
  Use command line flags (see `c.t.f.socks.SocksProxyFlags.scala`) instead.
  ``RB_ID=834634``

* finagle-core: Removed "closechans" and "closed" counters from `ChannelStatsHandler`.
  ``RB_ID=835194``

* finagle-core: Removed the "load" gauge from `StatsFilter` as it was duplicated
  by the "pending" gauge. ``RB_ID=835199``

* finagle-core: `c.t.finagle.NoStacktrace` is removed. Use `scala.util.control.NoStackTrace`
  instead. ``RB_ID=833188``

* finagle-core: `c.t.finagle.Failure.withStackTrace` is removed. Use system property
  `scala.control.noTraceSuppression` instead to fill stacktraces in Finagle's failures.
  ``RB_ID=833188``

* finagle-core: `c.t.f.filter.RequestSerializingFilter` is removed.
  Use `c.t.f.filter.RequestSemaphoreFilter` instead. ``RB_ID=839372``

* finagle-core: `SessionParams` no longer contains `acquisitionTimeout`. Instead, it
  was extracted into `ClientSessionParams`. ``RB_ID=837726``

* finagle-core: Changed visibility of PipeliningDispatcher to private[finagle].  Clients should
  not be affected, since it's not a part of the end-user API. ``RB_ID=843153``.

* finagle-core: Simplified and unified the constructors for FailureAccrualFactory into
  a single constructor. ``RB_ID=849660``

* finagle-http: Deprecate channelBufferUsageTracker in favor of maxRequestSize.
  ``RB_ID=831233``

* finagle-http: HttpClientDispatcher, HttpServerDispatcher, and
  ConnectionManager are no longer public. ``RB_ID=830150``

* finagle-redis: Deprecated methods have been removed from the client API.
  ``RB_ID=843455``

* finagle-redis: `c.t.f.redis.*Commands` traits are now package-private.
  ``RB_ID=843455``

* finagle-redis: Replace `ChannelBuffer` with `Buf` in client's:

    - `HashCommands`: ``RB_ID=843596``
    - `ListCommands`: ``RB_ID=844596``
    - `BtreeSortedSetCommands`: ``RB_ID=844862``
    - `HyperLogLogCommands`: ``RB_ID=844945``
    - `PubSubCommands`: ``RB_ID=845087``
    - `SetCommands`: ``RB_ID=845578``
    - `SortedSetCommands`: ``RB_ID=846074``

* finagle-thrift: As part of the migration off of `Codec`, remove
  `c.t.f.thrift.ThriftClientBufferedCodec` and `c.t.f.thrift.ThriftClientBufferedCodecFactory`
  which were used by `ClientBuilder.codec` and `ServerBuilder.codec`. Replace usage
  with `ClientBuilder.stack(Thrift.client.withBufferedTransport)`
  or `ServerBuilder.stack(Thrift.server.withBufferedTransport)`. ``RB_ID=838146``

* finagle-memcached: `c.t.f.memcached.Client` now uses `c.t.bijection.Bijection`
  instead of `c.t.u.Bijection`. ``RB_ID=834383``

* finagle-zipkin: Moved case classes and companion objects `Span`, `ZipkinAnnotation`,
  `BinaryAnnotation`, `Endpoint`, `Sampler` and `SamplingTracer` to finagle-zipkin-core.
  ``RB_ID=840494``

* finagle-mysql: Removed `c.t.f.exp.mysql.transport.MysqlTransporter`, as it was not useful for it
  to be public. ``RB_ID=840718``

Bug Fixes
~~~~~~~~~

* finagle-core: PipeliningDispatcher now serializes "write and enqueue Promise" so it's no longer
  possible for the wrong response to be given to a request. ``RB_ID=834927``

* finagle-http: Servers which aggregate content chunks (streaming == false) now return a 413
  response for streaming clients who exceed the servers' configured max request size.
  ``RB_ID=828741``

* finagle-mysql: `c.t.f.exp.mysql.PreparedCache` now closes prepared statements when no one holds
  a reference to the cached future any longer.  This fixes a race condition where the cached
  future could be evicted and the prepared statement closed while a user tries to use that
  prepared statement.  ``RB_ID=833970``

* finagle-netty4-http: Servers now see the correct client host address for requests. ``RB_ID=844076``

New Features
~~~~~~~~~~~~

* finagle-core: Added gauge, "scheduler/blocking_ms" measuring how much time,
  in milliseconds, the ``com.twitter.concurrent.Scheduler`` is spending doing blocking
  operations on threads that have opted into tracking. This also moves the
  "scheduler/dispatches" gauge out of TwitterServer into Finagle. ``RB_ID=828289``

* finagle-core: Added a FailureAccrualPolicy that marks an endpoint
  dead when the success rate in a specified time window is under the
  required threshold. ``RB_ID=829984``

* finagle-core: `StackServer` now installs an `ExpiringService` module by default. This
  allows servers to have control over session lifetime and brings the `StackServer` to
  feature parity with `ServerBuilder`.  ``RB_ID=837726``

* finagle-exp: Changed DarkTrafficFilter to forward interrupts to dark service. ``RB_ID=839286``

* finagle-http: ContextFilter and Dtab-extractor/injector logic has been moved from
  the http dispatchers into the client and server stacks. ``RB_ID=840600``

* finagle-mysql: Added a `withMaxConcurrentPreparedStatements` method to the client which lets you
  specify how many prepared statements you want to cache at a time.  ``RB_ID=833970``

* finagle-redis: Adds support for scripting commands. ``RB_ID=837538``

* finagle-netty4: SOCKS5 proxy support. ``RB_ID=839856``

* finagle-zipkin-core: A new module containing most of the functionality
  from finagle-zipkin, leaving finagle-zipkin with only Scribe specific code
  and a service loader. This allows for other transports to be implemented
  in separate modules. For example the upcoming finagle-zipkin-kafka.
  ``RB_ID=840494``

* finagle-thriftmux: Introduce a Netty4 implementation of mux and thrift-mux.
  ``RB_ID=842869``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: For SSLEngine implementations supplied via configuration or
  created by Finagle, the setEnableSessionCreation method is no longer called.
  The supplied value, true, is the default for JSSE implementations, and for
  other engines this can be an unsupported operation. ``RB_ID=845765``

* finagle-core: Pipelined protocols (memcached, redis) no longer prevent
  connections from being cut by interrupts.  Instead, interrupts are masked
  until a subsequent ten second timeout has expired without a response in the
  pipeline. ``RB_ID=843153``

* finagle-core: MonitorFilter now installs the parameterized monitor, and will
  no longer fail the request automatically if any exception is thrown
  synchronously (like if an exception is thrown in an onSuccess or onFailure
  block).  This removes a race, and makes Finagle more deterministic.
  ``RB_ID=832979``

6.35.0
------

Deprecations
~~~~~~~~~~~~
* finagle: remove unused finagle-validate and finagle-testers packages. ``RB_ID=818726``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: DeadlineFilter is now per-connection, so the max rejection percentage
  is not shared across clients. This prevents a single client from exceeding the rejection
  budget. ``RB_ID=813731``.

* finagle-core: The use of keytool in PEMEncodedKeyManager has been removed and instead the
  keystore is being loaded from the pkcs12 file. ``RB_ID=832070``

* finagle-http: Local Dtabs are now encoded into the `Dtab-Local` header.  `X-Dtab` headers
  may still be read but should be considered deprecated. ``RB_ID=815092``

* finagle-thrift: Removed duplicate "thrift" label on Thrift/ThriftMux scrooge-related
  server stats. ``RB_ID=816825``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~
* finagle-redis: Deprecated ChannelBuffer exposing apis for string commands. ``RB_ID=817766``.

* finagle-core: DefaultClient has been removed. Implementors should prefer `c.t.f.StackClient`
  ``RB_ID=812681``.

* finagle-core: When a client is created, its server set resolution is started eagerly.
  ``RB_ID=806940``

* finagle-core: Dentry now takes a Dentry.Prefix instead of a Path. ``RB_ID=813914``

* finagle-core: Remove `*.client.withTransport.socksProxy` param as part of the initiative to
  move to CLI flags based configuration for SOCKS. ``RB_ID=842512``

* finagle-thrift/thriftmux: Thrift servers constructed using `Thrift.serveIface` now use
  `Thrift.server.serveIface`. ThriftMux servers constructed using `ThriftMux.serveIface` now use
  `ThriftMux.server.serveIface`. ``RB_ID=824865``

* finagle-cache-resolver: `c.t.f.cacheresolver.ZookeeperCacheNodeGroup` has been removed from the
  API since we no longer check for the zookeeper data for the cache pool size to refresh for the
  changes in the serverset. ``RB_ID=811190``

New Features
~~~~~~~~~~~~

* finagle-http: http 1.1 running on netty4 is configurable via `c.t.finagle.netty4.http.exp.Netty4Impl`.
  It has not been tested in production and should be considered beta software. ``RB_ID=828188``

* finagle-core: Multi-line Dtabs may now contain line-oriented comments beginning with '#'.
  Comments are omitted from parsed Dtabs. ``RB_ID=818752``

* finagle-http: new stack params MaxChunkSize, MaxHeaderSize, and MaxInitialLineLength
  are available to configure the http codec. ``RB_ID=811129``

* finagle-mux: Mux now has support for fragmenting Tdispatch and Rdispatch payloads.
  This helps with head-of-line blocking in the presence of large payloads and allows
  long transmissions to be interrupted. ``RB_ID=794641``.

* finagle-core: Dtabs allow wildcard path elements in prefixes. ``RB_ID=813914``

* finagle-netty4: HTTP proxy support for any Finagle Netty 4 client. ``RB_ID=819752``

* finagle-core: Gauge for dns resolutions awaiting lookups. ``RB_ID=822410``

Bug Fixes
~~~~~~~~~

* finagle-http: Ensure that service closure is delayed until chunked response bodies
  have been processed. ``RB_ID=813110``

* finagle-stats: Ensure that histogram snapshotting does not fall behind if snapshot()
  is not called at least once per interval. ``RB_ID=826149``

6.34.0
------

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: GenSerialClientDispatcher fails pending and subsequent requests when
  its underlying transport closes. ``RB_ID=807590``

New Features
~~~~~~~~~~~~

* finagle-core: Include upstream/downstream addresses/client ids and request trace id
  in exceptions that extend `c.t.f.HasRemoteInfo` (including `c.t.f.SourcedException`),
  accessible via the `remoteInfo` value. ``RB_ID=797082``

* finagle-core: Introduce `c.t.f.service.ResponseClassifier` for HTTP servers,
  which allows developers to give Finagle the additional application specific knowledge
  necessary in order to properly classify responses.``RB_ID=800179``

* finagle: Export two new histograms: `request_payload_bytes` and `response_payload_bytes`
  for the following protocols: HTTP (non-chunked), Mux, ThriftMux and Thrift. ``RB_ID=797821``

* finagle-core: Define `c.t.f.Address` to represent an endpoint's physical location.
  Resolvers and namers may attach metadata such as weight to individual endpoint addresses.
  ``RB_ID=792209``

* finagle-http: Introduce convenience extractors to pattern match `c.t.f.http.Response.status`
  against the different categories. ``RB_ID=802953``

* finagle-http: Add `toBoolean` method in `StringUtil` to parse strings to boolean consistently.
  ``RB_ID=804056``

* finagle-http: Servers now actually decompress requests when decompression is turned on.
  ``RB_ID=810629``

* finagle-redis: Add support for SENTINEL commands. ``RB_ID=810663``

* finagle-redis: Support for Pub/Sub. ``RB_ID=810610``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.Codec.prepareConnFactory(ServiceFactory)` is marked `final`, override
  `c.t.f.Codec.prepareConnFactory(ServiceFactory, Stack.Params)` instead. ``RB_ID=797821``

* finagle-core: `c.t.f.Codec.newClientDispatcher(Transport)` is marked `final`, override
  `c.t.f.Codec.newClientDispatcher(Transport, Stack.Params)` instead. ``RB_ID=797821``

* finagle-core: Removed deprecations: ``RB_ID=800974``
  - Removed `c.t.f.Service.release`, replace usage with `Service.close()`.
  - Removed `c.t.f.ServiceFactory.make`, replace usage with `ServiceFactory.apply`.
  - Removed `c.t.f.ProxyServiceFactory`, replace usage with `ServiceFactoryProxy`.
  - Removed deprecated `c.t.f.service.FailureAccrualFactory` constructor.
  - Removed `c.t.f.netty3.ChannelBufferBuf.apply`, replace usage with `ChannelBufferBuf.Owned.apply`.
  - Removed `c.t.f.util.InetAddressUtil.Loopback`, replace usage with `java.net.InetAddress.getLoopbackAddress`.
  - Removed `c.t.f.tracing.TracingFilter`, replace usage with `TraceInitializationFilter` and `(Client|Server)TracingFilter`.

* finagle-core: `c.t.f.Addr.Bound.addr` type changed from `Set[SocketAddress]` to
  `Set[c.t.f.Address]`. We provide a migration guide below for the most common cases.

  Callers of `c.t.f.Addr.Bound.addr` must handle `Set[c.t.f.Address]` instead of
  `Set[SocketAddresses]`. If you do something with the `SocketAddress` and expect the underlying
  type to be `InetSocketAddress`, use `c.t.f.Address.Inet.addr` to get the underlying
  `InetSocketAddress`.

  `c.t.f.Addr` constructors and `c.t.f.Name.bound` method now accept `c.t.f.Address` instead
  of `SocketAddress`. For most cases, wrapping the `InetSocketAddress` in an `Address.Inet`
  will fix the compile error.

  Any other `SocketAddress` subclass is currently incompatible with `c.t.f.Address`. Instead,
  you should encode any additional information in the metadata field of `c.t.f.Address.Inet`
  or `c.t.f.exp.Address.ServiceFactory`. ``RB_ID=792209``

* finagle-core: Delete `c.t.f.ServiceFactorySocketAddress` and replace usages with
  `c.t.f.exp.Address.ServiceFactory`. ``RB_ID=792209``

* finagle-core: Delete `c.t.f.WeightedSocketAddress` and instead use
  `c.t.f.addr.WeightedAddress` to represent address weights. ``RB_ID=792209``

* finagle-core: `c.t.f.builder.ClientBuilder.hosts` takes a Seq of `InetSocketAddress` instead of
  `SocketAddress`. If you get a compile error, change the static type to `InetSocketAddress` if
  you can. Otherwise, cast it at runtime to `InetSocketAddress`. ``RB_ID=792209``

* finagle-core: `c.t.f.client.Transporter.EndpointAddr` takes a `c.t.f.Address` as its
  parameter instead of `SocketAddress`. ``RB_ID=792209``

* finagle-core: `c.t.f.service.FauilureAccrualFactory.Param(FailureAccrualPolicy)` is removed -
  it's not safe to configure Failure Accrual with a shareable instance of the policy, use
  `() => FailureAccrualPolicy` instead. ``RB_ID=802953``

* finagle-core: `$Client.withSessionQualifier.failureAccrualPolicy` has been removed from the API
  since it enables an experimental feature (use Stack's `.configured` API instead). ``RB_ID=802953``

* finagle-core: `c.t.f.service.exp.FailureAccrualPolicies` (Java-friendly API) has been removed -
  use `c.t.f.service.exp.FailureAccrualPolicy` instead.

* finagle-core: DefaultServer is removed. Protocol implementors should use StackServer instead.
  ``RB_ID=811918``

* finagle-memcached: `c.t.f.memcached.protocol.text.Memcached` no longer takes a `StatsReceiver`,
  pass it to a `(Client/Server)Builder` instead. ``RB_ID=797821``

* finagle-redis: `c.t.f.redis.Redis` no longer takes a `StatsReceiver`, pass it to a
  `(Client/Server)Builder` instead. ``RB_ID=797821``

* finagle-core: `c.t.f.http.MapHeaderMap` no longer takes a `mutable.Map[String, Seq[String]]` as
  a constructor parameter. `apply` method provides a similar functionality.

Bug Fixes
~~~~~~~~~

* finagle-core: Fixed `getAll` method on `c.t.f.http.MapHeaderMap`, now it is case insensitive.
  `apply` method was altering the provided header names. This is fixed it is now possible to
  iterate on the original header names.

6.33.0
------

New Features
~~~~~~~~~~~~

* finagle-core: Introduce the `c.t.f.service.PendingRequestFactory` module in the client Stack.
  The module allows clients to limit the number of pending requests per connection. It is disabled
  by default. ``RB_ID=795491``

* finagle-core: Introduce the `c.t.f.filter.ServerAdmissionControl` module in the server Stack,
  which is enabled through the param `c.t.f.param.EnableServerAdmissionControl`. Users can define
  their own admission control filters, which reject requests when the server operates beyond
  its capacity. These rejections apply backpressure and allow clients to retry requests on
  servers that may not be over capacity. The filter implementation should define its own logic
  to determine over capacity. One or more admission control filters can be installed through
  the `ServerAdmissionControl.register` method. ``RB_ID=776385``

* finagle-core: Introduce `c.t.f.service.ResponseClassifier` which allows developers to
  give Finagle the additional application specific knowledge necessary in order to properly
  classify them. Without this, Finagle can only safely make judgements about transport
  level failures. This is now used by `StatsFilter` and `FailureAccrualFactory` so that
  application level failures can be used for both success metrics and failure accrual.
  ``RB_ID=772906``

* finagle-core: Added a new 'Endpoints' section on client pages, listing the weights, paths,
  and resolved endpoints for each dtab.``RB_ID=779001``

* finagle-core: Introduce discoverable stack params which are available on every client/server
  via the `with`-prefixed methods. ``RB_ID=781833``

* finagle-memcached: Added `c.t.f.memcached.BaseClient.checkAndSet` which exposes the difference
  between a conflict and a not found result.

* finagle-mux: Add a Wireshark dissector that can decode Mux messages. ``RB_ID=779482``

* finagle-stats: Define flag `c.t.f.stats.statsFilterFile` as GlobalFlag[Set[File]] to take
  comma-separated multiple files. ``RB_ID=793397``

* finagle-mux: Tinit/Rinit are now available and permit feature negotiation. ``RB_ID=793350``

Deprecations
~~~~~~~~~~~~

* finagle-memcached: `c.t.f.memcached.BaseClient.cas` is deprecated in favor of the richer
  `checkAndSet` method.

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: All the deprecated exceptions from `Exceptions.scala` have been removed.
  ``RB_ID=774658``
* finagle-thrift: Remove the `framed` attributes from `c.t.f.Thrift.Client` and
  `c.t.f.Thrift.Server`.  This behavior may now be controlled with `c.t.f.Thrift.param.Framed`.
* finagle-core: Unused `c.t.f.builder.NonShrinkingCluster` has been removed.
  ``RB_ID=779001``

* finagle-thrift: `c.t.f.ThriftRichClient` has a new abstract protected method
  `responseClassifier: ResponseClassifier`. If your implementation does not need
  this, you can implement it with `ResponseClassifier.Default`. ``RB_ID=791470``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-thrift,thriftmux: Deserialization of Thrift responses now happens as part
  of service application which means that it will now be part of the latency reported by
  `StatsFilter`. The actual latency as perceived by clients will not have changed, but
  for clients that spend significant time deserializing and do not have higher level
  metrics this may come as a surprise. ``RB_ID=772931``

* finagle-mux,thriftmux: The default `closeTimeout` in ping based failure detection
  is changed from Duration.Top to 4 seconds, to allow a session to be closed by default
  when a ping response times out after 4 seconds. This allows sessions to be reestablished
  when there may be a networking issue, so that it can choose an alternative networking
  path instead. ``RB_ID=773649``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-thrift: Remove the `framed` attributes from `c.t.f.Thrift.Client` and
  `c.t.f.Thrift.Server`.  This behavior may now be controlled with `c.t.f.Thrift.param.Framed`.

6.32.0
------

NOT RELEASED

6.31.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `c.t.f.Server` now has a `serveAndAnnounce` method that accepts a `SocketAddress`
  as an address. ``RB_ID=758862``

* finagle-core: `c.t.f.service.Retries` now supports adding delay between each automatic retry.
  This is configured via the `Retries.Budget`. ``RB_ID=768883``

* finagle-core: FailureAccrualFactory now uses a FailureAccrualPolicy to determine when to
  mark an endpoint dead. The default policy, FailureAccrualPolicy.consecutiveFailures(),
  mimicks existing functionality, and FailureAccrualPolicy.successRate() operates on the
  exponentially weighted average success rate over a window of requests.``RB_ID=756921``

* finagle-core: Introduce `c.t.f.transport.Transport.Options` to configure transport-level options
  (i.e., socket options `TCP_NODELAY` and `SO_REUSEADDR`). ``RB_ID=773824``

* finagle-http: `c.t.f.http.exp.Multipart` now supports both in-memory and on-disk file uploads.
  ``RB_ID=RB_ID=769889``

* finagle-netty4: Hello World. Introduce a `Listener` for Netty 4.1. This is still considered beta.
  ``RB_ID=718688``

* finagle-netty4: Introduce `ChannelTransport` for Netty 4.1. ``RB_ID=763435``

* finagle-thrift: `c.t.f.ThriftRichClient` implementations of `newServiceIface`
  method that accept a `label` argument to pass to the `ScopedStats` instance. ``RB_ID=760157``

* finagle-stats: Added `c.t.f.stats` now has a `statsFilterFile` flag which will read a denylist
  of regex, newline-separated values. It will be used along with the `statsFilter` flag for stats
  filtering. ``RB_ID=764914``

Deprecations
~~~~~~~~~~~~

* finagle-core: the #channelFactory method of `c.t.f.builder.ServerBuilder` has been deprecated
  in favor of the `c.t.f.netty3.numWorkers` flag. ``RB_ID=718688``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: The behavior for `c.t.f.util.DefaultMonitor` has changed such that
  unhandled exceptions are propagated to `c.t.u.RootMonitor` except for
  `c.t.f.Failures` with a log `Level` below INFO. ``RB_ID=758056``

* finagle-core: The metrics for requeues `requeue/requeues`, `requeue/budget` and
  `requeue/budget_exhausted` have moved under retries. They are now `retries/requeues`,
  `retries/budget` and `retries/budget_exhausted`. ``RB_ID=760213``

* finagle-core: `c.t.f.service.RetryFilter` and `c.t.f.service.RetryExceptionsFilter`
  now default to using a `RetryBudget` to mitigate retry amplification on downstream
  services. The previous behavior can be achieved by explicitly passing in
  `RetryBudget.Infinite`. ``RB_ID=766302``

* finagle-core: `c.t.f.factory.TrafficDistributor` now suppresses changes when a bound
  address is updated from a valid set to an error. Instead, it continues using stale
  data until it gets a successful update.

* finagle-http: Unhandled exceptions from user defined HTTP services are now converted
  into very basic 500 responses so clients talking to those services see standard HTTP
  responses instead of a dropped connection. ``RB_ID=755846``

* finagle-memcached: Moved metrics from underlying `KetamaPartitionedClient` for Memcached clients
  to share the same scope of the underlying finagle client. ``RB_ID=771691``

* finagle-mux: `com.twitter.finagle.mux.ThresholdFailureDetector` is turned on by
  default. ``RB_ID=756213``

* finagle-serversets: The `c.t.f.serverset2.Zk2Resolver` now surfaces `Addr.Pending`
  when it detects that its underlying ZooKeeper client is unhealthy. Unhealthy is defined
  as non-connected for greater than its 'unhealthyWindow' (which defaults to 5 minutes).
  ``RB_ID=760771``

* finagle-serversets: The `c.t.f.serverset2.ZkSession` now uses an unbounded semaphore to
  limit to 100 outstanding zookeeper requests at any one moment. ``RB_ID=771399``


Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `BackupRequestLost` is no longer itself an `Exception`. Use
  `BackupRequestLost.Exception` in its place. ``RB_ID=758056``

* finagle-core: Replaced `c.t.f.builder.ClientConfig.Retries` with
  `c.t.f.service.Retries.Policy`. ``RB_ID=760213``

* finagle-core: A deprecated `c.t.f.CancelledReadException` has been removed.
  ``RB=763435``

* finagle-http: `c.t.f.http.exp.Multipart.decodeNonChunked` has been removed from
  the public API. Use `c.t.f.http.Request.multipart` instead. Also
  `c.t.f.http.exp.Multipart.FileUpload` is no longer a case class, but base trait
  for `Multipart.InMemoryFileUpload` and `Multipart.OnDiskFileUpload`. ``RB_ID=769889``

* finagle-mux: `c.t.f.FailureDetector.apply` method is changed to private scope,
  to reduce API surface area. Using `FailureDetector.Config` is enough to config
  session based failure detection behavior. ``RB_ID=756833``

* finagle-mux: `closeThreshold` in `c.t.f.mux.FailureDetector.ThresholdConfig` is
  changed to `closeTimeout`, from an integer that was used as a multiplier to time
  duration. This makes it easier to config. ``RB_ID=759406``

Bug Fixes
~~~~~~~~~

* finagle-thrift: `c.t.f.ThriftRichClient` scoped stats label is now threaded
  properly through `newServiceIface` ``RB_ID=760157``

6.30.0
------

New Features
~~~~~~~~~~~~

* finagle-core: `com.twitter.finagle.client.LatencyCompensator` allows its
  default Compensator value to be set via an API call. This allows
  libraries to set defaults for clients that have not configured this module.
  ``RB_ID=750228``

* finagle-core: New Resolver `com.twitter.finagle.FixedInetResolver` extends
  InetResolver by caching successful DNS lookups indefinitely. It's scheme is 'fixedinet'.
  This is used by clients or resolvers that do not want or expect
  host->ip map changes (such as the zk2 resolver and twemcache client).
  ``RB_ID=753712``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `RetryPolicy.tries` now uses jittered backoffs instead of
  having no delay. ``RB_ID=752629``

* finagle-core: `FailureAccrualFactory` uses jittered backoffs as the duration
  to mark dead for, if `markDeadFor` is not configured. ``RB_ID=746930``

* finagle-core: The transit latency (transit_latency_ms) and deadline budget
  (deadline_budget_ms) stats are now only recorded for servers, not for
  clients anymore, since they're only meaningful for servers. ``RB_ID=75268``

* finagle-http: Clients sending requests with payloads larger than the server
  accepts (default 5MB) now receive a HTTP 413 response instead of a channel
  closed exception.  ``RB_ID=753664``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `TimerFromNettyTimer` is renamed to `HashedWheelTimer` and
  the constructor accepting `org.jboss.netty.util.Timer` made private. For
  compatibility, `HashedWheelTimer` has additional constructors to match
  those provided by `org.jboss.netty.util.HashedWheelTimer`. ``RB_ID=748514``

* finagle-httpx / finagle-httpx-compat: Renamed to finagle-http and
  finagle-http-compat respectively. This changes the package names, e.g.:
  com.twitter.finagle.httpx to com.twitter.finagle.http. ``RB_ID=751876``

* finagle-core: Marked `HandletimeFilter` private[finagle], and renamed it to
  `ServerStatsFilter`. ``RB_ID=75268``

* finagle-zipkin: Drop `c.t.zipkin.thrift.Annotation.duration` and associated thrift field
  `c.t.f.thrift.thrift.Annotation.duration`. ``RB_ID=751986``

* finagle-stress: Project has been removed from Finagle. ``RB_ID=752201``

* finagle-swift: Project has been moved off of Finagle to
  https://github.com/finagle/finagle-swift . ``RB_ID=752826``

6.29.0
------

Deprecations
~~~~~~~~~~~~

* finagle-http: Deprecated in favour of finagle-httpx and now removed.

New Features
~~~~~~~~~~~~

* finagle-core: Provides a `RetryFilter` which takes a
  `RetryPolicy[(Req, Try[Rep])]` and allows you to retry on both "successful"
  requests, such as HTTP 500s, as well as failed requests. The `Req`
  parameterization facilitates using the request to determine if retrying is
  safe (i.e. the request is idempotent).

* finagle-httpx: Experimental support `multipart/form-data` (file uploads)
  decoding via `c.t.f.httpx.exp.Multipart`. ``RB_ID=730102``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `InetResolver.bind` will now succeed if any hostname resolution
  succeeds. Previous behavior required that all hosts are successfully resolved.
  ``RB_ID=737748``

* finagle-core: DNS lookups in InetResolver are no longer cached
  within Finagle according to `networkaddress.cache.ttl`; we rely
  instead on however caching is configured in the JVM and OS. ``RB_ID=735006``

* finagle-core: After being revived, a `FailureAccrualFactory` enters a
  'probing' state wherein it must successfully satisfy a request before
  accepting more. If the request fails, it waits for the next `markDeadFor`
  period. ``RB_ID=747541``

* finagle-serversets: DNS lookups in Zk2Resolver are no longer
  cached within Finagle according to `networkaddress.cache.ttl`;
  instead they are cached indefinitely. ``RB_ID=735006``

* finagle-redis: c.t.f.Redis now uses a pipelined dispatcher along with
  a concurrent load balancer to help eliminate head-of-line blocking.

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `RetryingFilter`, which takes a RetryPolicy[Try[Nothing]]` and
  is invoked only on exceptions, has been renamed to `RetryExceptionsFilter`.
  `RetryExceptionsFilter` is a subclass of `RetryFilter`, which takes a
  `RetryPolicy[(Req, Try[Rep])]` and allows you to retry on both "successful"
  requests, such as HTTP 500s, as well as failed requests. The `Req`
  parameterization facilitates using the request to determine if retrying is
  safe (i.e. the request is idempotent).

* finagle-core: Name.all is now private to `com.twitter.finagle`.

* finagle-memcached: Unified stack-based construction APIs and cleanup internal
  constructors. In particular, `KetamaClient` was removed and `KetamaPartitionClient`
  and `KetamaFailureAccrualFactory` are now sealed inside Finagle. See
  [[com.twitter.finagle.Memcached]] for how to construct a finagle-memcached client.

* finagle-redis: Port the c.t.f.Redis protocol object to the StackClient API.
  A redis client can now be constructed and configured like the rest of the
  finagle subprojects.

6.28.0
------

New Features
~~~~~~~~~~~~

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Weights are no longer supported by the load balancers. They are moved
  one level above and interpreted by a new module, the `TrafficDistributor`. This
  frees the balancers to have non-linear load metrics. It also changes the semantics
  of weights. They are now normalized by size of endpoints that share the same weight
  and interpreted proportional to offered load (however, they can still be though of,
  roughly, as multipliers for traffic). ``RB_ID=677416``

* finagle-core: The RequestSemaphoreFilter now sheds load by dropping the tail of the queue
  and failing it with a `Failure.Restartable`. Previously, the filter had an unbounded
  queue but now the default size is 0 (i.e. no queueing). The dropped requests are in
  turn requeued by Finagle clients with protocol support (e.g. Http, ThriftMux).
  ``RB_ID=696934``

* finagle-core: `ServerBuilder.ServerConfig.BindTo`, `ServerBuilder.ServerConfig.MonitorFactory`,
  and `ServerBuilder.ServerConfig.Daemonize`, are now private to `com.twitter.finagle.builder`. ``RB_ID=730865``

* finagle-memcachedx: Renamed to finagle-memcached.

* finagle-stats: Standard deviation ("$statName.stddev") is no longer exported.
  ``RB_ID=726309`` (follow up to ``RB_ID=717647``)

* finagle-serversets: `namer/bind_latency_us` stat now counts only
  time in name resolution, not service acquisition.
  `namer/{dtabcache,namecache,nametreecache}/misstime_ms` stats are
  no longer exported. ``RB_ID=730309``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.jsr166y` has been replaced with Java 7 API. finagle: Replace JSR166y
  with Java 7 API. ``RB_ID=720903``

* finagle-core: `LoadBalancerFactory` no longer takes endpoints with weights as
  per the decoupling mentioned in runtime changes. ``RB_ID=677416``

* finagle-core: `RequestSemaphoreFilter.Param` now accepts a `com.twitter.concurrent.AsyncSemaphore`
  instead of an integer representing the max concurrency. ``RB_ID=696934``

* finagle-core: removed `c.t.f.asyncDns` flag and `c.t.f.SyncInetResolver`; DNS resolution is
  now always asynchronous. ``RB_ID=734427``

* finagle-core: `ClientBuilder.ClientConfig.DefaultParams`, `ClientBuilder.ClientConfig.DestName`,
  `ClientBuilder.ClientConfig.GlobalTimeout`, `ClientBuilder.ClientConfig.Daemonize`, and
  `ClientBuilder.ClientConfig.MonitorFactory` are now private to `com.twitter.finagle.builder`.
  `ClientBuilder.ClientConfig.Retries` is now private to `com.twitter`. ``RB_ID=727245``

* finagle-httpx: `Method` no longer has an extractor. To access the name of
  custom methods, use `toString`. ``RB_ID=722913``

* finagle-mux: `c.t.f.mux.exp.FailureDetector` and `c.t.f.mux.exp.sessionFailureDetector` are
  moved out of exp package into mux package. ``RB_ID=725350``

6.27.0
------

New Features
~~~~~~~~~~~~

* finagle-http: Support nacks between Finagle Http clients and servers. When a server fails
  with retryable exceptions (exceptions wrapped by `Failure.rejected`), it sends back a "Nack"
  response, i.e. 503 Response code with a new "finagle-http-nack" header. This allows clients
  to safely retry failed requests, and keep connections open. ``RB_ID=705948``

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: FailFast parameter renamed from `onOrOff` to `enabled`. ``RB_ID=720781``

* finagle-core: When evaluating NameTree unions, return components of the union in Ok state rather
  than waiting for all components to be Ok. This enables resilience of unions when part of the
  tree cannot be resolved. ``RB_ID=697114``

* finagle-stats: Standard of deviation is no longer calculated. It is exported as a constant 0.0
  for "$statName.stddev". ``RB_ID=717647``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~
* finagle-stream: Introduce StreamRequest as a replacement for Netty's
  HttpRequest, and converted the rest of the public API to not leak
  other Netty types (notably ChannelBuffer is replaced by Buf). ``RB_ID=695896``

* finagle-core: Dtab does not implement the Namer interface anymore. Use
  `c.t.f.naming.DefaultInterpreter` to bind a name via a Dtab. Support for Dtab entries starting
  with /#/ has been removed. `c.t.f.Namer.bindAndEval` has been removed. Use
  `c.t.f.Namer.resolve` instead. ``RB_ID=711681``

* finagle: `LoadService` and `ThriftRichClient` migrated off of deprecated `ClassManifest`
  to `ClassTag`. ``RB_ID=720455``

6.26.0
------

Deprecations
~~~~~~~~~~~~

* finagle-memcached: Deprecated in favor of finagle-memcachedx and now removed.

New Features
~~~~~~~~~~~~

* finagle-httpx: Support nacks between Finagle Http clients and servers. When a server fails
  with retryable exceptions (exceptions wrapped by `Failure.rejected`), it sends back a "Nack"
  response, i.e. 503 Response code with a new "finagle-http-nack" header. This allows clients
  to safely retry failed requests, and keep connections open. ``RB_ID=670046``

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~
* finagle-core: Moved netty3 specific things into a netty3 namespace. For
  these symbols, the namespace translation follows this pattern:
  `c.t.f.$MODULE._ => c.t.f.netty3.$MODULE._.` ``RB_ID=691746``

* finagle-core: Define `WeightedSocketAddress` as a case class. Add
  `WeightedSocketAddress.extract` method to extract weight. ``RB_ID=614228``

* finagle-core: Constructing a new Balancer that can be injected into a Finagle client
  was unnecessarily complex and non-uniform. We removed the legacy constructors around
  defining the collection of endpoints and simplified the interface to `LoadBalancerFactory`.
  Now, `com.twitter.finagle.loadbalancer.Balancers` defines the collection of balancer
  constructors. ``RB_ID=660730``

* finagle-core: Aperture can no longer be enabled via command line flags. Configuring
  per-client settings globally is generally not a good idea and we're working to remove
  these flags from Finagle. Use the constructors in `com.twitter.finagle.loadbalancer.Balancers`
  to create an instance that can be injected into a client. ``RB_ID=663194``

* finagle-core: The default load balancer has changed to p2c from heap. ``RB_ID=693450``

* finagle-core: `Service.isAvailable` and `ServiceFactory.isAvailable` is finalized.
  `Service.status` and `ServiceFactory.status` supersedes `isAvailable` usage since 6.24.0 release.
  ``RB_ID=678588``

* finagle-core: `ClientBuilder.failureAccrual` method is removed. Use `ClientBuilder.failureAccrualFactory`
  instead. ``RB_ID=689076``

* finagle-core: Stack param `ClientBuilder.ClientConfig.FailureAccrualFac` is removed.
  Use `ClientBuilder.failureAccrualFactory` instead. ``RB_ID=689076``

* finagle-exception: `com.twitter.finagle.exception.ExceptionReporter` is no longer used
  as the default `com.twitter.finagle.util.ReporterFactory`. ``RB_ID=674646``

* finagle-kestrel: Replace deprecated finagle-kestrel package with finagle-kestrelx.
  ``RB_ID=667920``

* finagle-core: Add new method `noFailureAccrual` on `ClientBuilder` that completely disables
  `FailureAccrualFactory` in the underlying stack. ``RB_ID=689076``

New Features
~~~~~~~~~~~~

- finagle-thrift: Support for finagle Services per thrift method.

6.25.0
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.builder.Server` now implements `c.t.f.ListeningServer`.

* finagle-core: `c.t.f.Server.serveAndAnnounce` with a `ServiceFactory` had
  its first argument renamed to `name` from `forum`.

* finagle-core: Add an attribute map to `c.t.f.Addr.Bound`.

* finagle-core: `c.t.f.builder.ClientConfig.FailFast` has moved to
  `c.t.f.FailFastFactory.FailFast`.

* finagle-core: NoBrokersAvailableException now has two Dtab constructor
  arguments, both the base and local Dtabs.

* finagle-core: `c.t.f.Failure` convenience constructors (e.g., `Failure.Cause`,
  `Failure.Rejected`) were removed in favor of uniform flag treatment, and clean
  separation of attributes from interpretation of those attributes.

* finagle-core: `ExitGuard` usage is now private to finagle.

* finagle-core: `c.t.f.service.TimeoutFilter.module` is now split into
  `TimeoutFilter.clientModule` and `TimeoutFilter.serverModule`.

* finagle-core: remove deprecated `c.t.f.builder.ClientBuilder.stack` taking a
  `Stack.Params => Client[Req1, Rep1]`.

* finagle-core: StackRegistry.Entry takes different constructor arguments, and the
  name has been bundled in with the Stack.Params.  StackRegistry.Entry is only used
  internally, so this should be relatively inexpensive.  Similarly, StackRegister#register
  has also had a small change to its method signature along the same lines.

* finagle-http: deprecated methods in `c.t.f.http.HttpMessageProxy` have been removed.

* finagle-memcached / finagle-memcachedx: move TwitterCacheResolver
  and related objects to new finagle-cacheresolver package.

* finagle-memcached / finagle-memcachedx: failureAccrual param in ReplicationClient
  is changed from type (Int, Duration) to (Int, () => Duration), to allow flexibility
  to config duration. Also see `markDeadFor` change in finagle-core in the "New Features"
  section below.

* finagle-memcached / finagle-memcachedx: MigrationClientTest now uses
  ServerCnxnFactory from com.twitter.zk rather than NIOServerCnxn.Factory from
  org.apache.zookeeper.server.

* finagle-mux: `c.t.f.mux.RequestNackedException` is removed in favor of a standard
  Failure (`c.t.f.Failure.Rejected`).

* finagle-ostrich4: Switched dependency to finagle-httpx from finagle-http.

* finagle-serversets: ZkInstance in tests now uses ServerCnxnFactory from
  com.twitter.zk rather than NIOServerCnxn.Factory from
  org.apache.zookeeper.server.

* finagle-stats: Switched dependency to finagle-httpx from finagle-http.

* finagle-mysql: PreparedStatements are now more type-safe! The type signature of
  PreparedStatements has changed from Seq[Any] => Future[Result] to Seq[Parameter] =>
  Future[Result]. Parameter represents objects that are serializable by finagle-mysql.
  In most cases, scalac should transparently wrap your arguments in Parameter when
  applying a PreparedStatement. However, in cases where this doesn't happen you
  can explicitly wrap them using `Parameter.wrap`.

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: `com.twitter.finagle.service.StatsFilter` no longer requires a
  `com.twitter.finagle.stats.RollupStatsReceiver` for correct behaviour, and
  providing one will double count failures.

* finagle-core: `com.twitter.finagle.factory.TimeoutFactory` will fail with
  a retryable `com.twitter.finagle.Failure` when it times out.

* finagle-core: `com.twitter.finagle.pool.WatermarkPool` will fail with
  an interrupted `com.twitter.finagle.Failure` when it is interrupted while
  waiting or trying to establish a connection.  It has previously failed with
  a `com.twitter.finagle.WriteException` when trying to establish a
  connection, but it's incorrect to retry on an interruption.

* finagle-core: `com.twitter.fiangle.RetryPolicy`'s `RetryableWriteException`
  and `WriteExceptionsOnly` will not retry on `com.twitter.finagle.Failure`s
  that are marked `InterruptedBy`, even if they are `Retryable`.

* finagle-core: The error message provided by `c.t.f.NoBrokersAvailableException`
  prints both the base and local Dtabs.

* finagle-core: Stats produced by `com.twitter.finagle.factory.BindingFactory`
  are now scoped with the "namer" prefix rather than "interpreter". The total
  latency associated with Name binding is now recorded in the "bind_latency_ms"
  stat.

* finagle-core: The "service_creation/service_acquisition_latency_ms" stat
  produced by `com.twitter.finagle.factory.StatsFactoryWrapper` no longer
  includes time spent in name resolution, which is now covered by
  "namer/bind_latency_us" as discussed above.

* finagle-core: added transit_latency_ms and deadline_budget_ms stats.

* finagle-core: Automatic retries (requeues) are now credited as a ratio of
  requests over a window of time, instead of a fixed limit. The stats scope
  has also changed from "automatic" to "requeues".

Deprecations
~~~~~~~~~~~~

* finagle-core: `c.t.f.builder.Server.localAddress` is deprecated in favor of
  `c.t.f.ListeningServer.boundAddress`.

New Features
~~~~~~~~~~~~

* finagle-core: `Fail fast <https://twitter.github.io/finagle/guide/FAQ.html#why-do-clients-see-com-twitter-finagle-failedfastexception-s>`_
  is now `configurable <https://twitter.github.io/finagle/guide/FAQ.html#configuring-finagle6>`_
  on Stack-based clients via the `com.twitter.finagle.FailFastFactory.FailFast` param.

* finagle-core: `com.twitter.finagle.service.StatsFilter` is now configurable with an
  `com.twitter.finagle.stats.ExceptionStatsHandler` to customize how failures are recorded.

* finagle-core: It should be safe to match on `com.twitter.finagle.Failure.InterruptedBy`
  to tell if a `com.twitter.util.Future` failed due to being interrupted.

* finagle-core: `markDeadFor` in c.t.f.service.FailureAccrualFactory.Param is changed from
  Duration type to () => Duration. So it's flexible for clients to pass in a function that
  specifies Duration. For example, c.t.f.service.FailureAccrualFactory provides a function
  that adds perturbation in durations. Stack-based API and c.t.f.builder.ClientBuilder
  support both types for client configuration. For example,

    ::

      Thrift.client.configured(FailureAccrualFactory(5, () => 1.seconds))
      // or
      Thrift.client.configured(new FailureAccrualFactory(5, 1.seconds)).

    c.t.f.client.DefaultClient does not support Duration type in failureAccrual anymore.

* finagle-core: improved Java compatiblity for `c.t.f.Stack.Params` / `c.t.f.Stack.Parameterized`.

* finagle-core: Introduce the ability to add metadata to a bound `com.twitter.finagle.Addr`.

* finagle-core: Introduce per-address latency compensation.  Clients may be configured with
  a 'Compensator' function that uses the client's address metadata to adjust connection and
  request timeouts.  This can be used, for instance, to account for speed-of-light latency
  between physical regions.

* finagle-core: Introduce per-address stats scoping.  Clients may be configured with
  a `com.twitter.finagle.client.StatsScoping.Scoper` function that uses the client's
  address metadata to adjust the scope of client stats. This can be used, for instance,
  to properly scope client stats for a Name that resolves to a Union of distinct clusters.

* finagle-core: A convenient method `Client.newService(dest: String, label: String)` was added.

* finagle-core: ExitGuard now has an 'explainGuards' method to provide a human-readable
  description of exit guards that are still active.

* finagle-http(x): Two missing params were added: `Decompression` and `CompressionLevel`. Both
  client and server may be configured with either `configured` method or `withDecompression`/
  `withCompressionLevel`.

* finagle-mysql: Add support for MySQL transactions.

* finagle-stats: A new HostStatsReceiver type is added and used for per host stats.
  It is loaded through LoadService and used by the Stack-based API as a default
  param. Per host stats can be turned on through `com.twitter.finagle.loadbalancer.perHostStats`
  flag, and is exported to the "/admin/per_host_metrics.json" route in twitter-server.

* finagle-stats: Improved compatibility when migrating from
  `Ostrich <https://github.com/twitter/ostrich/>`_ stats via two flags:
  ``com.twitter.finagle.stats.useCounterDeltas=true`` and
  ``com.twitter.finagle.stats.format=ostrich``. If these flags are both set,
  HTTP requests to ``/admin/stats.json`` with the ``period=60`` query string
  parameter will replicate Ostrich's behavior by computing deltas on counters
  every minute and formatting histograms with the same labels Ostrich uses.

* finagle-memcached(x): Add `c.t.f.memcached.Memcached` which provides a Stack
  based Memcache client that uses pipelining.

Bug Fixes
~~~~~~~~~

* finagle-core: `c.t.f.Server.serveAndAnnounce` for a `Service` had its usage
  of `name` and `addr` transposed.

Miscellaneous Cleanup
~~~~~~~~~~~~~~~~~~~~~

* finagle-protobuf: Move entire unused subproject to the `Finagle
  organization on GitHub <https://github.com/finagle/finagle-protobuf>`_.


6.24.0
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Remove `c.t.f.client.StackClient.Role.loadBalancer`, which
  was unused and duplicated by `c.t.f.loadbalancer.LoadBalancerFactory.role`.

* finagle-core: `c.t.f.Namer.orElse` was removed; composing Namers
  may be accomplished by constructing an appropriate Dtab.

* finagle-core: removed experimental `enum` / `expand` from
  `c.t.f.Namer` and `alt` / `union` from `c.t.f.Dtab`.

* finagle-http: Remove `c.t.f.http.CheckRequestFilter` along with
  `c.t.f.http.CheckHttpRequestFilter`. The functionality has been
  added to `c.t.f.http.codec.HttpServerDispatcher`. In addition,
  the codecError in `c.t.f.http.BadHttpRequest` has been replaced
  with the exception thrown by the HttpServerCodec.

* finagle-httpx: Remove deprecated code, limited scope of access on internal
  classes.

* finagle-mux: `c.t.f.mux.lease.exp.WindowedByteCounter` no longer
  calls `Thread.start()` in its constructor. This should be now be
  done by the caller.

* finagle-mux: The experimental session API is discontinued.

* finagle-mux: Introduce new Request and Response types for mux services.
  The new mux request includes a `destination` path so that, which corresponds
  to the `destination` field in Tdispatch requests. Furthermore, these new
  types expose `c.t.io.Buf` instead of Netty's ChannelBuffers.

* finagle-thrift,finagle-thriftmux: `c.t.f.Thrift.Client`, `c.t.f.Thrift.Server`,
  `c.t.f.ThriftMux.Client` and `c.t.f.ThriftMux.Server` have their
  `TProtocolFactory` configured via a `c.t.f.thrift.param.ProtocolFactory`
  `Stack.Param`.

* finagle-thriftmux: `c.t.f.ThriftMux.Client` now has its `ClientId`
  configured via a `c.t.f.thrift.param.ClientId` `Stack.Param`.


* Traces (``com.twitter.finagle.tracing.Trace``) lose their local-state mutating methods:
  ``Trace.clear``, ``Trace.pushId``, ``Trace.setId``, ``Trace.setTerminalId``, ``Trace.pushTracer``,
  ``Trace.pushTracerAndSetNextId``,
  ``Trace.state_=``, and ``Trace.unwind``.
  Let-bound versions of these are introduced in their stead.
  This makes it simple to ensure that state changes are properly delimited;
  further, these are always guaranteed to be delimited properly by Finagle.

    ::

      Trace.setTracer(tracer)
      codeThatUsesTracer()

      // Let-bound version:
      Tracer.letTracer(tracer) {
        codeThatUsesTracer()
      }

* Context handlers (``com.twitter.finagle.Context``) are removed.
  They are replaced by the use of marshalled request contexts
  (``com.twitter.finagle.context.Contexts.broadcast``).
  Marshalled request contexts do not require the use of service loading,
  so their use no longer requires build system coordination.
  We show Finagle's trace context:
  the first version uses the old context handler mechanism;
  the second uses ``Contexts.broadcast``.

    ::

      // The old context handler for Finagle's tracing context. Note that this
      // also required the file
      // finagle-core/src/main/resources/META-INF/services/com.twitter.finagle.ContextHandler
      // to contain the fully qualifed class path of the below object.
      class TraceContext extends ContextHandler {
        val key = Buf.Utf8("com.twitter.finagle.tracing.TraceContext")

        def handle(body: Buf) {
          // Parse 'body' and mutate the trace state accordingly.
        }

        def emit(): Option[Buf] = {
          // Read the trace state and marshal to a Buf.
        }
      }

      // New definition. No service loading required.
      private[finagle] val idCtx = new Contexts.broadcast.Key[TraceId] {
        val marshalId = Buf.Utf8("com.twitter.finagle.tracing.TraceContext")

        def marshal(id: TraceId): Buf = {
          // Marshal the given trace Id
        }

        def tryUnmarshal(body: Buf): Try[TraceId] = {
          // Try to marshal 'body' into a trace id.
        }
      }


Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-mux: Mark the ServiceFactory available again if the underlying
  Service is no longer available.  This permits it to be closed and reused.

* finagle-mux: Rename the "lease_counter" counter to "leased" on mux clients.

Deprecations
~~~~~~~~~~~~

* finagle-core: Deprecated the mechanisms of FailureAccrual that use
  factory Transformers.  It's better to just use the Params to
  configure the existing FailureAccrualFactory.  However, if you've
  actually written your own failure accrual transformer that's
  significantly different, then you can do stack.replace() to swap it
  in.

* finagle-memcached: Have cas() operation return false on NotFound()
  state instead of throw IllegalStateException

New Features
~~~~~~~~~~~~

* finagle-core: All `Stack.Param`s used in `ClientBuilder` and
  `ServerBuilder` are now publicly exposed for configuration
  parity.

* finagle-mux: Drain mux servers properly, so that shutdowns can be
  graceful.

* finagle-core: Introduce `Service.status` which supersedes
  `Service.isAvailable`. `Service.status` is a fine-grained
  health indicator. The default definition of
  `Service.isAvailable` is now defined in terms of
  `Service.status`; this definition will soon be made
  final.

* finagle-mux: Inject bound residual paths into mux requests.

* *Request contexts.* Request contexts replace the direct use of
  com.twitter.util.Local and of com.twitter.finagle.Context.
  Request contexts are environments of request-local bindings;
  they are guaranteed to be delimited by Finagle,
  and their API admits only properly delimited binding.
  They come in two flavors:
  Contexts.local are always local to handling a single request;
  bindings in Contexts.broadcast may be marshalled and transmitted across process
  boundaries where there is protocol support.
  Currently, both Thrift and Mux (and thus also ThriftMux)
  support marshalled contexts.
  See `com.twitter.finagle.contexts.Context` for more details.


6.23.0
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: `c.t.f.Stackable`s maintained state about which parameters were
  accessed via `get`. This was error prone and violated the assumption that `Stack`s
  are immutable data structures. We removed this in favor of annotating modules with
  parameters. The abstract classes for Stackables were also simplified. Now we only have
  `Module` and `ModuleN` variants which are more convenient for most definitions.
  Since this is an advanced API, it should not impact standard usage of finagle.
* finagle-core: Update ConcurrentRingBuffer to use ClassTag instead of ClassManifest;
  rename size argument to capacity.

Deprecations
~~~~~~~~~~~~

* `ServerBuilder.stack[Req1, Rep1](mk: Stack.Params => Server[Req1, Rep1])` is
  deprecated in favor of
  `ServerBuilder.stack[Req1, Rep1](server: Stack.Parameterized[Server[Req1, Rep1]])`

Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* `finagle`: Improve allocation semantics for uses of Buf.

6.22.0
------

Breaking API Changes
~~~~~~~~~~~~~~~~~~~~

* finagle-core: Removed unused `com.twitter.finagle.service.ProxyService`. It wasn't
  sufficiently general to be used outside of finagle, and was no longer used
  in finagle.
* Removed TLSEngine, and replaced it with two, TLSClientEngine, and
  TLSServerEngine, where TLSServerEngine is the same as TLSEngine, and
  TLSClientEngine takes a SocketAddress instead of (). Additionally,
  the Netty3TransporterTLSConfig now takes a function SocketAddress => Engine,
  instead of () => Engine.

New Features
~~~~~~~~~~~~

* finagle-core: BroadcastStatsReceiver, introduce specialized implementation
* finagle-core: Introduce gauges in SummarizingStatsReceiver
* finagle-core: Introduce Transport#copyToWriter
* finagle-core: Make base Dtab used in BindingFactory a Stack.Param
* finagle-core: Proper decay in experimental ewma load metric
* finagle-core: Simplify Stack{Client, Server} and unify around them


Runtime Behavior Changes
~~~~~~~~~~~~~~~~~~~~~~~~

* finagle-core: Add support for non-URLClassloaders to LoadService
* finagle-core: clear locals before entering server dispatch loop
* finagle-core: Defer DNS Resolution in InetResolver to FuturePool
* finagle-core: for paths starting with /#/, skip rewrites where prefix is /
* finagle-core: include name resolution in tracing request span
* finagle-core: Properly wrap some IOException into ConnectionFailedException
* finagle-core: Scope InetResolver's stats properly
* finagle-http: Send "Connection: close" header while dispatcher is closing
* finagle-http: Set content length header when appropriate
* finagle-memcached: Use interruptible future for the client request readiness
* finagle-stats: Add content-type response header to JsonExporter
* finagle-thrift: Add back connection_preparation_latency stat in Thrift
* finagle-thriftmux: Record protocol as a gauge rather than a counter

Documentation
~~~~~~~~~~~~~

* finagle-core: Add Scaladocs for ChannelBufferBuf and BufChannelBuffer
* finagle-core: link to the FAQ in FailedFastException
* finagle-serversets: Defer DNS resolution for endpoints to InetResolver
* finagle-thrift{,mux}: Clarified with* deprecation warning
* Fix minor issues and missing code blocks in Finagle documentation

Optimization
~~~~~~~~~~~~

* finagle-core: GlobalFlag isn't caching the property value
* finagle-core: recursive-descent Path / NameTree / Dentry / Dtab parsers to reduce heap allocation

Bug Fixes
~~~~~~~~~

* finagle-core: Fix a deadlock in Contexts
* finagle-core: Fix breaking ABI change in SummarizingStatsReceiver
* finagle-core: Fix bug in computing array index in SummarizingStatsReceiver
* finagle-core: Fix build.properties location for maven builds
* finagle-core: Fix synchronization in LeasedFactory
* finagle-core: Fix tracing with Stack{Client, Server}
* finagle-core: Make FailedFastException an instance variable.
* finagle-core: Synchronized access to the Stackable mutable.params map
* finagle-http: Fix CookieMap.rewriteCookieHeaders()
* finagle-http: Fix the memory leak in HttpClientDispatcher
* finagle-mysql: Fix timestamp fractional seconds encoding
* finagle-mysql: Properly heed timezone when extracting TimestampValue
* mux: clear contexts after receive, not in 'finally' clause

6.21.0
------

- `finagle`: Upgrade to scala_2.10
- `finagle-core`: Add weighted address support to InetResolver.
- `finagle-core`: Adds ClientRegistry.expAllRegisteredClientsResolved
- `finagle-core`: Attach Dtab.local to NoBrokersAvailableException in filter
- `finagle-core`: Attempt to add dtab stats to all clients and servers
- `finagle-core`: Avoid SOCKS proxy for both loopback and link-local addresses
- `finagle-core`: Bind names lazily under Alt so we don't do needless lookups
- `finagle-core`: DelayedFactory to detachable implementation
- `finagle-core`: Fix a race in load balancer initialization
- `finagle-core`: Improve loadbalancer logging
- `finagle-core`: Introduce ewma load metric
- `finagle-core`: Log state for ewma load metric
- `finagle-core`: Redesign of twitter-server admin page
- `finagle-core`: Register StackClient with Path dest in ClientRegistry as evaluated against Dtab.base Problem
- `finagle-core`: Rewrote ServiceLoader to avoid creating
- `finagle-core`: SingletonPool: a single-service pool with good semantics
- `finagle-core`: Zipkin tracing for name resolution
- `finagle-core`: fix synchronization in SummarizingStatsReceiver
- `finagle-core`: reusing pool: isAvailable === underlying.isAvailable
- `finagle-docs`: A better picture for filters. (Replaces hand-drawn monstrosity.)
- `finagle-docs`: Add Scaladocs for all public exception classes in Finagle.
- `finagle-docs`: Add a section to the FAQ regarding FailedFastExceptions
- `finagle-exp`: Generify the DarkTrafficFilter problem
- `finagle-guide`: Document stabilizing serverset metrics.
- `finagle-http`: Add support for a Dtab-Local header
- `finagle-http`: Fail requests with invalid X-Dtab headers
- `finagle-http`: Fail requests with invalid X-Dtab headers
- `finagle-http`: TextualContentCompressor should compress textual Content-Type's even when containing a charset
- `finagle-mux`,thriftmux: explain why the files are named Netty3
- `finagle-mux`: Better gc avoidance flags for experimentation
- `finagle-mux`: Enable gc avoidance for parallel gc
- `finagle-mux`: Improve mux's handling of client disconnects
- `finagle-mux`: Improved Garbage Collection Avoidance Docs
- `finagle-mux`: docs and minor cleansing in Message subclasses
- `finagle-mysql`: Switches PreparedCache over to util-cache FutureCache
- `finagle-serversets`: Add support for getEphemerals ZooKeeper call
- `finagle-serversets`: Memoize getChildrenWatchOp instead of globPrefixWatchOp
- `finagle-serversets`: Reuse ObjectMapper in JSON parser.
- `finagle-serversets`: add latency metrics for ServerSet2.
- `finagle-zipkin`: Reduce allocations when tracing

6.20.0
------

- `finagle`: Smattering of minor cleanups in util and finagle
- `finagle`: Upgrade sbt to 0.13
- `finagle`: Upgrade to Netty 3.9.1.1.Final
- `finagle-core`: Add NameTree.Fail to permit failing a name without fallback
- `finagle-core`: Add a generic DtabStatsFilter
- `finagle-core`: Add a singleton exception and a counter in WatermarkPool
- `finagle-core`: DefaultClient in terms of StackClient
- `finagle-core`: Disable Netty's thread renaming
- `finagle-core`: Fix CumulativeGauge memory leak
- `finagle-core`: Fix negative resolution in Namer.global
- `finagle-core`: Fixed ChannelStatsHandler to properly filter exceptions
- `finagle-core`: Forces finagle-core to use ipv4 network stack
- `finagle-core`: Improve `Failure.toString`
- `finagle-core`: Include path and Dtab.local in NoBrokersAvailableException
- `finagle-core`: Log exceptions caught by ChannelStatsHandler
- `finagle-core`: Make timer-based DNS resolution as default of InetResolver
- `finagle-core`: Reader and getContent symmetry
- `finagle-core`: Reduces log level for common exceptions
- `finagle-core`: Register clients centrally
- `finagle-doc`: Add fintop to companion projects list on Finagle website
- `finagle-http`: Don't emit (illegal) newlines in lengthy dtab header values
- `finagle-http`: Fix code style from an open-source contribution
- `finagle-http`: Migrate from specs to scalatest
- `finagle-kestrel`: Make transaction abort timeout configurable in MultiReader
- `finagle-mux`: Added extra client logging
- `finagle-mux`: Fix broken draining behavior
- `finagle-mux`: Improve granularity of rate to bytes/millisecond
- `finagle-serversets`: Handle errors that occur when fetching endpoints
- `finagle-serversets`: Increase ZK session timeout to 10 seconds
- `finagle-serversets`: Merge WeightedSocketAddresses with same host:port but different weight in Stabilizer
- `finagle-serversets`: Synchronize bug fixes & test coverage across ZK facades
- `finagle-swift`: Fixes pants build warning
- `finagle-thrift`: Add explicit dependency on libthrift
- `finagle-thrift`: Remove usage of java_sources, should be able to depend on it normally

6.19.0
------

- `finagle-core`: Allow trailing semicolons in dtabs
- `finagle-core`: Rescue exceptions thrown by filter in `Filter.andthen(Filter)`
- `finagle-core`: StackClient, StackClientLike don't leak underlying In, Out types
- `finagle-doc`: Clarify cancellation
- `finagle-doc`: Fix broken link in document
- `finagle-doc`: Fix name footnote in finagle Names docs
- `finagle-http`: Buf, Reader remove Buf.Eof; end-of-stream is None
- `finagle-http`: Prepend comment to JSONP callbacks
- `finagle-http`: Removing specs from the CookieMapSpec test.
- `finagle-kestrel`: Make failFast configurable in Kestrel codec
- `finagle-mysql`: Ensure mysql specific tracing is composed.
- `finagle-mysql`: Finagle MySQL PreparedStatement accepts Value types as params.
- `finagle-serversets`: Identity Providers for Serverset2
- `finagle-thriftmux`: Add withProtocolFactory API endpoint
- `finagle-thriftmux`: Don't reuse InMemoryStatsReceiver in the same test

6.18.0
------

- `finagle-*`: release scrooge v3.16.0
- `finagle-*`: release util v6.18.0
- `finagle-core`: Add `description` field to com.twitter.finagle.Stackable trait
- `finagle-core`: Add a Flag to turn on per-host stats
- `finagle-core`: Add a service acquisition latency stat to StatsFactoryWrapper
- `finagle-core`: Don't support empty path elements in com.twitter.finagle.Path
- `finagle-core`: Improves FailFastFactory documentation
- `finagle-core`: Make c.t.f.Failure a direct subclass of Exception
- `finagle-core`: Skip SOCKS proxy when connecting to loopback address
- `finagle-core`: Use Monitor from caller's context in DefaultTimer
- `finagle-http`: Add "Enhance Your Calm" and "Too Many Requests" HTTP status codes
- `finagle-http`: Add exp.HttpServer, which allows request limits to be configured
- `finagle-http`: Change Request#params to a memoized def
- `finagle-http`: Stream request body
- `finagle-kestrel`: Add Name-based methods for MultiReader construction
- `finagle-memcached`: Expose the client type `KetamaClient` in the `build()` API
- `finagle-mux`: GC Avoidance Algorithm
- `finagle-mux`: Hook up GC avoidance to servers
- `finagle-mux`: Move UseMux.java to the correct directory
- `finagle-serversets`: Randomizes backoff interval in ZK2
- `finagle-serversets`: Start resolution eagerly in ZK2
- `finagle-stats`: Add a stat-filtration GlobalFlag
- `ostrich`: release ostrich v9.5.2
- `user guide`: Add Google Analytics tracking code
- `user guide`: Add sections about review process and starter issues
- `user guide`: Update Finagle adopter list on user guide website
- `wily`: Add Dtab expansion

6.17.0
------

- `finagle`: Add list of Finagle adopters
- `finagle`: Upgrade third-party dependencies
- `finagle-core`: Add `Addr.Neg` to the user guide's list of Addr types
- `finagle-core`: Added Failure support for sourcing to finagle
- `finagle-core`: ClientBuilder should turn per-host stats off by default (matching new Client building API).
- `finagle-core`: Implement DefaultServer in terms of StackServer
- `finagle-core`: Improve the Dtab API
- `finagle-core`: Prevent scoping stats with the empty-string
- `finagle-core`: Rolls up the /tries scope properly
- `finagle-core`: ServerStatsReceiver and ClientStatsReceiver can now update their root scope
- `finagle-core`: fix race case in DelayedFactory
- `finagle-core`: introduce AbstractResolver
- `finagle-core`: remove need for hostConnectionLimit when using ClientBuilder#stack
- `finagle-core`: widen to type for ServerBuilder#stack
- `finagle-core`: widen type of ClientBuilder#stack
- `finagle-doc`: Removed a line from conf.py
- `finagle-http`: DtabFilter should always clear dtab headers
- `finagle-http`: add HOST header for CONNECT method
- `finagle-http`: scala 2.10 compatible tests
- `finagle-memcached`: filter out one more cancelling request exception in failure accrual
- `finagle-memcached`: remove empty test
- `finagle-mux`: Improve Mux server close behavior, control messages to non-Mux clients
- `finagle-mux`: Marked a gc test as flaky
- `finagle-mux`: Modifies MuxService to essentially be a Service[Spool[Buf], Spool[Buf]] Problem
- `finagle-mux`: Rm ClientHangupException in favor of CancelledRequestException
- `finagle-mysql`: Retrieving a timestamp from the DB nw creates a timestamp in UTC
- `finagle-mysql`: fix for issue where time was not being returned in UTC for binary protocol
- `finagle-serversets`: Prevent gauges from being garbage collected
- `finagle-thrift`: Blackhole control messages sent to non-mux Thrift clients
- `finagle-thriftmux`: Add per-connection protocol-usage stats
- `finagle-thriftmux`: Add stats to identify ThriftMux clients and servers
- `finagle-thriftmux`: Propagate Contexts from non-ThriftMux clients
- `finagle-thriftmux`: add ClientBuilder#stack compatibility and make APIs symmetric
- `finagle-thriftmux`: pass along ClientId with ClientBuilder API

6.16.0
------

- `finagle-core`: Add Stack#remove
- `finagle-core`: Add a copy constructor to Stack{Client, Server}
- `finagle-core`: Fixed a typo in scaladoc https://github.com/twitter/finagle/pull/264
- `finagle-core`: Implement ClientBuilder in terms of StackClient
- `finagle-core`: Invert the `cancelOnHangup` value passed to MaskCancelFilter.Param
- `finagle-core`: Liberate Failure
- `finagle-core`: Log all services loaded by c.t.finagle.util.LoadService
- `finagle-core`: Minor c.t.app.ClassPath/c.t.f.util.LoadService cleanup
- `finagle-core`: Properly close sockets on shutdown
- `finagle-core`: Properly scope stats by label in Stack{Client,Server}
- `finagle-core`: Remove `Stack{Server,Client}.transformed`
- `finagle-core`: Scoped the RollupStatsReceiver carefully
- `finagle-core`: Thread through Codec#newClientTransport in ClientBuilder
- `finagle-core`: Update to netty-3.9.1.Final
- `finagle-example`: Add Java Thrift client and server
- `finagle-http`: Add Csv,Xls,Zip to finagle MediaType
- `finagle-http`: Adds tls support to finagle 6 apis
- `finagle-http`: Set the response content-length header to 0 in ExceptionFilter.
- `finagle-kestrel`: Add Thrift support to Kestrel MultiReader in Finagle-Kestrel
- `finagle-mux`: Cleaned up build information
- `finagle-mux`: GC Avoidance primitives
- `finagle-mux`: Move exp.MuxClient and exp.MuxServer
- `finagle-mux`: Record tracing info when Mux is enabled
- `finagle-mux`: Refactor Session to make closing a Session uniform
- `finagle-mux`: Render mux clients leasable
- `finagle-redis`: Added support for redis MOVE command.
- `finagle-serversets`: Reduce the number of intermediate datastructures
- `finagle-thriftmux`: Add ThriftMux.withClientId
- `finagle-thriftmux`: Maintain legacy client and server names
- `finagle-{core,thrift,mux}`: Clean up contexts, delimit Locals

6.15.0
------

- `finagle-core`: Fixed DefaultClient to use the base close method
- `finagle-core`: Fix a race condition when closing in DefaultServer
- `finagle-serversets`: memoize path parses in ServerSet2
- `finagle-mux`: remove references to org.jboss.netty.util.CharsetUtil
- `finagle-http`: create HttpTransport in codec
- `finagle-http`: fix basic authentication with special characters
- `finagle-http`: temporary fix for prematurely expiring streaming responses
- `finagle-core`: don't discard outstanding readq elements in ChannelTransport
- `finagle-core`: Add Socks Proxy Authentication support
- `finagle-doc`: fix image size in client stack figure.
- `finagle-stats`: unregister cumulative gauges when all references have been collected
- `finagle-core`: fix truncation in ChannelBufferBuf#slice()
- `finagle-stats`: upgrade to the latest version of metrics
- `finagle-stats`: Enable cumulative gauges in MetricsStatsReceiver
- `finagle-mysql`: Move mysql testing out of finagle-mysql
- `finagle-serversets`: serverset namer - synthesize nodes for each endpoint
- `finagle-http`: fix HttpClientDispatcher
- `finagle-core`: transport should be considered closed if it is failed
- `finagle-core`: Improve the failure for cancelled requests in the ClientDispatcher
- `finagle-core`: LocalScheduler - add LIFO option
- `finagle-core`: don't join after interrupt in Exitguard.Unguard()
- `finagle-serversets`: Replaces Op with Activity Problem
- `finagle-mysql`: implement builder using StackClient.
- `finagle-core`: Make LoadService not fail if a sub-dir is not readable
- `finagle-core`: Make com.twitter.finagle.Name an ADT
- `finagle-core`: curry `newDispatcher` in Stack{Client, Server}
- `finagle-thriftmux`: Add a flag for enabling ThriftMux
- `finagle-doc`: improved rastering of logos
- `finagle-core`: Retry on com.twitter.util.TimeoutException
- `finagle-core`: introduce ForkJoinScheduler
- `finagle-serversets`: facade for ZooKeeper libraries

6.14.0
------

- `finagle-*`: Add com.twitter.io.Charsets and replace the use of org.jboss.netty.util.CharsetUtil
- `finagle-benchmark`: Fix caliper failures due to new guava
- `finagle-core`: Disable Monitor usage of in Netty3Listener
- `finagle-core`: Enforce usage of c.t.finagle.util.DefaultLogger
- `finagle-core`: Fix a Netty3Timer capitalization bug
- `finagle-core`: Fixed unresolved promises in client dispatcher
- `finagle-core`: Implement ServerBuilder in terms of StackServer.
- `finagle-core`: Introduce 2-level caching in the name interpreter
- `finagle-core`: Introduce Failure interface (internally)
- `finagle-core`: Introduce StackServer
- `finagle-core`: Introduce a flag for debug tracing
- `finagle-core`: Make StackClient symmetric to StackServer
- `finagle-core`: Parse names into trees; introduce separate evaluation.
- `finagle-core`: Remove redundant Netty3Timer param def
- `finagle-core`: Resolver.resolve throws IllegalArgumentException on logical name
- `finagle-core`: RetryPolicy filter, limit, combine
- `finagle-core`: Thread through NullReporterFactory in ServerBuilder.
- `finagle-core`: Use DefaultLoadBalancerFactory
- `finagle-core`: Use JDK6-friendly RuntimeException constructor in Failure
- `finagle-doc`: README refresh
- `finagle-doc`: Refresh client stack docs
- `finagle-memcached`: Ketama memcache: accept weighted addresses
- `finagle-mux`: Add server-side Stack wiring for Mux and ThriftMux
- `finagle-mysql`: Proper prepared statement support.
- `finagle-serversets`: Add Read-Only ZK support to zk2 Resolver
- `finagle-serversets`: Zk2: deliver events serially in their own thread
- `finagle-thrift`: workaround libthrift TBinaryProtocol.writeBinary bug
- `finagle-zipkin`: Include service name in all traces

6.13.1
------

- `finagle-core`: Case insensitive Dtab headers in HTTP codec
- `finagle-core`: Introduce Stack.Params#contains
- `finagle-docs`: address small style nits
- `finagle-http`: support reading params in content body for HTTP methods other than POST and PUT
- `finagle-memcached`: add ketamaclient initial readiness before the first request
- `finagle-serversets`: Disable retry behavior but turn exception into negative resolution.
- `finagle-serversets`: Stabilizer: don't consider Pending update successful
- `finagle-stats`: use java.lang.Double in addGauge()
- `finagle`: Add `cause` Throwables for all ConnectionFailedExceptions
- `finagle`: Fix Travis-CI integration.
- `finagle`: Swap Stack.Node and Stack.Leaf args in toString formatting
- `finagle`: Update 3rdparty library versions
- `finagle`: Upgrade birdcage to guava 16
- `finagle`: upgrade ostrich to 9.4.2
- `finagle`: upgrade util to 6.13.2

6.13.0
------

- `finagle-core`: ForkJoin scheduler: first draft
- `finagle-doc`: Update URL to Finagle blog post. Motivation: Outdated URL
- `finagle-http`: compress text-like content-types by default
- `finagle-memcached`: Mark test "not migrating yet" as flaky
- `finagle-mux`: don't delegate empty dtabs
- `finagle-mysql`: Better failure handling for the dispatcher
- `finagle-ostrich4`: Make OstrichExporter compatible with Ostrich CommandHandler.
- `finagle-redis`: Add SINTER command to redis client
- `finagle-redis`: Allow an empty string as a hash field value
- `finagle-redis`: Fixed the empty string issue of MBULK_REPLY
- `finagle-serverset`: ServerSets2: reset value on SyncConnected
- `finagle-stats`: Upgrade metrics dependency to the latest version.
- `finagle-thrift`: Do not rely on ThreadLocal'd ClientIds in TTwitterFilter
- `finagle-thrift[mux]`: Reintroduce ClientIdContext by default
- `finagle-zipkin`: TraceId invariance
- `finagle/S2`: introduce address stabilization, stats
- `finagle`: Add Mux and ThriftMux Clients based on com.twitter.finagle.Stack
- `finagle`: fix dependency problem with multiple version of serversets
- `finagle`: Fix the sbt doc generation (and tests)
- `finagle`: upgrade Netty to 3.8.1
- `finagle`: upgrade ostrich to version 9.4.0
- `finagle`: upgrade util to version 6.13.0

6.12.2
------

- `finagle`: release scrooge version 3.12.3
- `finagle-exception`: Drop scrooge in favor of pre-generated Thrift Java source code.
- `finagle-zipkin`: Drop scrooge in favor of pre-generated Thrift Java source code.

6.12.1
------

- `finagle`: release ostrich version 9.3.1
- `finagle`: release util version 6.12.1
- `finagle`: Upgrade everyone to the new c.t.common.server-set

6.12.0
------

- `finagle-core`: Add a `ServerBuilder.safeBuild(ServiceFactory)` method for Java compatibility
- `finagle-core`: Add basic Scaladocs for all Filters and Services
- `finagle-core`: close Name Var observation on service close
- `finagle-core`: com.twitter.finagle.Stack: initial version
- `finagle-core`: Fix race condition in server dispatcher draining state machine
- `finagle-core`: low hanging fruit
- `finagle-core`: Make `Resolver.evalLabeled` private[finagle]
- `finagle-core`: Option to enable thread pool scheduler in finagle"
- `finagle-core`: Record Finagle version in tracing information
- `finagle-core`: Remove a long-forgotten java file
- `finagle-core`: Separate stats scopes for service-creation and request failure
- `finagle-core`: TimeoutFactorySpec => TimeoutFactoryTest
- `finagle-core`: Users don't get NPE on directories without permissions
- `finagle-core`: Weights, weighted load balancer, memoization
- `finagle-core`: Write `Throwables.mkString` in terms of ArrayBuffer instead of Seq
- `finagle-doc`: How do I change my timeouts in the Finagle 6 APIs?
- `finagle-example`: Port finagle-example thrift to new style APIs
- `finagle-http`: Add Dtab filter in RichHttp
- `finagle-http`: enable tracing on finagle 6 http api
- `finagle-kestrel`: Fix match-exhaustiveness issue in `DecodingToResponse.parseResponse`
- `finagle-kestrel`: Use Var[Addr] as underlying cluster representation
- `finagle-memcache`: Add a parameter to disable host ejection from KetamaFailureAccrualFilter
- `finagle-memcached`: include response type for IllegalStateException
- `finagle-mux`: Adds lease support to mux clients
- `finagle-mysql`: Embeddable MySql support for Unit/Integration Testing in Finagle-MySql
- `finagle-serversets`: prevent MatchError when resolving zk path + endpoint
- `finagle-serversets`: Use ephemeral ports and `Var.sample` in tests
- `finagle-serversets`: Use the watch method instead of the now deprecated monitor method
- `finagle-tracing` /zipkin: remove some allocations
- `finagle`: update dependencies com.twitter.common*
- `finagle`: update ostrich to 9.3.0
- `finagle`: update util to 6.12.0
- `finagle`: Use a more descriptive message for when client name resolution is negative

6.11.1
------

- `finagle-core`: Masks cancellation on PipeliningDispatcher
- `finagle-doc`: Fix finagle-websocket link on Finagle docs website.
- `finagle-http`: Http client dispatcher
- `finagle-kestrel`: Add abort command to kestrel client

6.11.0
------

- `finagle-core`: Add a "tries" scoped StatsFilter to ClientBuilder.
- `finagle-core`: Allow clean shutdown for insoluble address in DelayedFactory.
- `finagle-core`: Better exception message for resolver not found issue.
- `finagle-core`: Introduce P2CLB: O(1), fair-weighted, concurrent load balancer
- `finagle-core`: Refactor dispatchers.
- `finagle-core`: Set interest ops when reading in ChannelTransport.
- `finagle-core`: Skip hostStatsReceiver rollup if it's null in DefaultClient.
- `finagle-core`: Untyped ChannelTransports.
- `finagle-example`: Use new `request.headers()` Netty API.
- `finagle-http`: Proper streaming dispatch.
- `finagle-kestrel`: Make ReadHandle an abstract class for better Java compatibility.
- `finagle-serversets`: Fix com.twitter.common.zookeeper.server-set dependency discrepancy.
- `finagle-serversets`: Introduce ServerSet2.
- `finagle-serversets`: Weight/priority vectors in ServerSet2.
- `finagle-stream`: Use dispatch logic.
- `finagle-stress`: Use BridgedThreadPoolScheduler.
- `finagle-thrift`: Add TFinagleBinaryProtocol
- `finagle-thriftmux`: Drop dependency on finagle-ostrich4.
- `finagle-thriftmux`: Remove scrooge2 dependency.
- `finagle`: Use Future.before.
- `finagle`: adds a section to the FAQ explaining the com.twitter.common situation.
- `finagle`: s/setValue(())/setDone()/g

6.10.0
------

- `finagle-core`: Fix ServiceTimeoutException naming.
- `finagle-core`: Increase default tcpConnectTimeout to 350ms.
- `finagle-core`: Remove memory leak for never satisfied promises in DelayedFactory.
- `finagle-memcached`: Add ClientBuilder APIs that use Name instead of Group.
- `finagle`: Daemonize threads in ClientBuilders used in finagle.

6.9.0
-----

- `finagle-core`: Avoid creating a new 'NoStacktrace' array for each exception.
- `finagle-core`: Better support for negative Addr resolution in StabilizingAddr.
- `finagle-core`: Make Dtab.base changeable.
- `finagle-core`: Move mask cancel filter over to Future#mask.
- `finagle-exp`: New warmup filter.
- `finagle-ostrich4`: Deprecate Netty methods.
- `finagle-serversets`: Better handling of permission exceptions in ZkResolver.
- `finagle-testers`: Small latency evaluation framework.
- `finagle`: Adhere to scala 2.10 pattern matching strictness.

6.8.1
-----

- `finagle`: Upgrade ostrich to 9.2.1
- `finagle`: Upgarde util to 6.8.1
- `finagle`: Upgarde scrooge to 3.11.1

6.8.0
-----

- `finagle-serversets`: Higher fidelity addrs from ZK We do the best we can with the current server set implementation.
- `finagle-mysql`: better type support
- `finagle-http`: Allow Integer to extract negative number
- `finagle-redis`: Decode nested multi-bulk replies
- `finagle-redis`: Allow expirations in the past
- `finagle-core`: bump Netty to 3.8.0.Final
- `finagle`: Return empty string when resolving an unlabeled address
- `finagle`: Don't re-scope StatsReceivers per request failure
- `finagle-thrift`: Unconditionally set TraceId and ClientId on servers
- `finagle-core`: Take -com.twitter.server.serversetZkHosts flag (for tunnelling)
- `finagle-native`: tomcat-native-1.1.27
- `finagle-mysql`: fix readLengthCodedBinary to read longs
- `finagle-testers`: library for integration testing of finagle services
- `finagle`: scaladoc warning cleanup
- `finagle-doc`: documented all of the stats in finagle-core
- `finagle-serversets`: bump server-set dependency to 1.0.56
- `finagle-stats`: adds a cautious registration to HttpMuxer / adds a default metrics endpoint to twitter-server
- `finagle-core`: verifies that statsfilter has the correct behavior
- `finagle-serversets`: Add support for parsing a !shardId on the end of a serverset address
- `finagle-http`: Use Reader for streaming
- `finagle-core`: no longer makes an anonymous exception in DefaultClient
- `finagle-core`: Using system class loader does not work when run inside sbt
- `finagle-core`: add pool_num_waited counter to WatermarkPool
- `finagle-core`: Protocol support for Wily: HTTP, thrift, mux (& thus thriftmux).
- `finagle-core`: respect standard socksProxyHost / socksProxyPort properties in ClientBuilder and Netty3Transporter default args
- `finagle-core`: buffers requests until Var[Addr] is in a ready state
- `finagle-core`: Add putLong and getLong functions to util.ByteArrays
- `finagle-core`: don't blow up if we don't have a resolvable host name
- `finagle-core`: rm allocation in RichChannelFuture.apply's operationComplete
- `finagle-core`: remove Var.apply; introduce Var.sample
- `finagle-thriftmux`: Support ClientIds at the protocol level
- `finagle-kestrel`: memory allocation improvements
- `finagle-http`: allow PUT requests to use RequestParamMap.postParams
- `finagle-memcached`: more performance and less allocations in Decoder

6.7.4
-----

- `finagle-core`: Fail ChannelTransport read queue before closing the connection
- `finagle-mux`: Add session for mux message passing and bidirectional RPC
- `finagle-zipkin`: Depend on scrooge-core instead of scrooge-runtime

6.7.2
-----

- `finagle-core`: LoadService: skip empty lines
- `finagle-core`: Improve GC profile of Var and Group
- `finagle-core`: added a simple defaultpool test to show how it works + more docs
- `finagle-core`: removes commented out casting filter
- `finagle-mysql`: move protocol into a dispatcher and port to new style apis.

6.7.1
-----

- `finagle-*`: Tagging various tests as flaky
- `finagle-*`: Fix and reenable some formerly-flaky tests Now that Time.withXXXX is threadsafe
- `finagle-*`: Move Var to util
- `finagle-*`: Provide generic request contexts
- `finagle-*`: Use scrooge3 and up-to-date scrooge-maven-plugin settings
- `finagle-*`: upgrade Netty to 3.7.0.Final
- `finagle-core`: Add UnwritableChannel stat to ChannelStatsHandler
- `finagle-core`: Add a test for Proc exception-swallowing
- `finagle-core`: Better interface for RetryPolicy
- `finagle-core`: Ensure FIFO ordering when delivering updates to StabilizingGroup
- `finagle-core`: Export a health stat in StabilizingGroup
- `finagle-core`: Fix a race condition in ListeningSever.announce and don't announce 0.0.0.0
- `finagle-core`: Fix sourcing for SourcedExceptions
- `finagle-core`: Group: communicate via Var[Set[T]]
- `finagle-core`: Improve HeapBalancer req distribution when N=2
- `finagle-core`: Introduce staged names
- `finagle-core`: Let the number of cores be specified on the command line
- `finagle-core`: Make socket writable duration stats more useful
- `finagle-core`: Migrate RetryingFilter tests from specs to scalatest
- `finagle-core`: Move the connections gauge into ChannelStatsHandler for GC
- `finagle-core`: Reuse a single exception instance in ServerDispatcher
- `finagle-core`: Update com.twitter.finagle package documentation
- `finagle-doc`: Add Client/Server Anatomy to docs
- `finagle-doc`: Link to "Your Server as a Function"
- `finagle-http`: Proper CORS implementation on Finagle
- `finagle-http`: SPNEGO http auth (for Kerberos) with an example implementation for internal services
- `finagle-memcached`: Exclude CancelledRequestException and CancelledConectionException or cache client failure accrual
- `finagle-memcached`: Read key remapping config from ZooKeeper
- `finagle-memcached`: ZK based cache node group implementation
- `finagle-redis`: SRandMember, send command without count when None
- `finagle-serversets`: Mark ZookeeperServerSetCluster as deprecated
- `finagle-thrift`: Add a request context for ClientId
- `finagle-thrift`: Set the client id in thriftmux

6.7.0
-----

Release process failed. Rolled forward to 6.7.1.

6.6.2
-----

- `finagle-core`: Configurable loadBalancer for ClientConfig
- `finagle-core`: Fix the memory leak due to the GlobalStatsReceiver
- `finagle-core`: Inet util, bind to all if no host is provided
- `finagle-core`: Make Future.never a val instead of a def
- `finagle-memcached`: Fix tracing ClientRecv timestamp
- `finagle`: New ostrich version

6.6.0
-----

- `finagle-core`: Add a RetryPolicy constant for retrying on ChannelClosedExceptions.
- `finagle-core`: ChannelSnooper: Print exceptions via Logger
- `finagle-core`: Finagle client/server APIs: s/target/addr/g
- `finagle-core`: Introduce swappable schedulers, ThreadPool scheduler.
- `finagle-core`: Replacing ostrich-specific Stats with StatsReceiver interface
- `finagle-core`: Tests for BroadcastStatsReceiver
- `finagle-core`: adding service name to a service exception
- `finagle-core`: fix file mispelling for StabilizingGroup.scala
- `finagle-core`: fix memory leak in Group#fromCluster
- `finagle-core`: refactor use of InetSocketAddress group and deprecate use of cluster in cache client
- `finagle-http`: Add Access-Control-Expose-Headers to Finagle CORSFilter
- `finagle-http`: Fix Path comments, improve sbt definition
- `finagle-http`: Http connection manager: close synchronously
- `finagle-http`: Make behavior of HttpMuxer match its javadoc
- `finagle-http`: Mix in Proxy with HttpMessageProxy
- `finagle-http`: bump request/response sizes to 5 megs
- `finagle-memcached`: Memcached Ketama Client Builder: Add group support that is compatible with oldlibmemcached , creates CacheNodes with ipAddress instead of hostname.
- `finagle-memcached`: Update jackson to 2.2.2
- `finagle-memcached`: allow custom key in ketama client
- `finagle-memcached`: better exception messages for invalid keys
- `finagle-memcached`: migration client
- `finagle-ostrich4`: improve perf of Counter and Stat
- `finagle-swift`: experimental swift-based thrift server and client An experimental annotation based proxy and dispatcher for thrift, using Facebook???s swift for serialization.
- `finagle-thrift`: breaking out finagle, higher-kinded-type interface
- `finagle-thrift`: fix finagle-thrift rich to work with scrooge 3.6.0
- `finagle-thrift`: move scrooge-runtime to scrooge
- `finagle-thrift`: remove use of deprecated generated ostrich ThriftServer
- `finagle-zipkin`: Add newlines to scribe message
- `finagle-zipkin`: Use better metric names for error stats
- `finagle-zipkin`: optimize scribe logging to not need slf4j

6.5.2
-----

- finagle-ostrich4: set stats export URI to stats.json
- finagle-core: Introduce StabilizingGroup which cautiously removes items from a Group
- finagle-core: Add group method to ClientBuilder
- finagle-core: Add service/client name to ServiceTimeoutException
- finagle-mux: Don't allocate headers via direct buffers
- finagle-http: Implement missing modifiers in Http case class
- finagle-memcache: added support for twemcache commands

6.5.1
-----

- `finagle-http`: Move routing by path and method function to RoutingService
- `finagle-redis`: Switching from Strings to ChannelBuffers for Scored Zrange commands
- `finagle-stats`: Upgrade metrics to 0.0.9
- `finagle-stream`: Revert "Stream interface (i.e. chunked) for HTTP message bodies"
- `finagle`: Fix documentation about Java & Future
- `finagle`: Refresh OWNERS/GROUPS in all subdirectories
- `finagle`: Upgrade Netty version to 3.6.6.Final
- `finagle`: Upgrade ostrich version to 9.1.2
- `finagle`: Upgrade util version to 6.3.7
- `finagle-redis`: Implement new SET syntax introduced in redis 2.6.12
- `finagle-core`: Open connection threshold et al

6.5.0
-----

- `finagle-core`: fix a bug in Trace.unwind in ruby
- `finagle-core`: speed up tracing when a trace is unsampled.
- `finagle-redis`: implement PEXPIRE/PEXPIREAT/PTTL
- `finagle-redis`: Make the multiple argument version of zAdd a separate method.
- `finagle-serverset`: create a ZkAnnouncer
- `finagle-thriftmux`: make thriftmux server back compatible with thrift framed codec
- `finagle-zipkin`: Performance improvement for finagle-zipkin
- `finagle`: Gizzard: Some followup deps alignment to fix deployment classpath issues
- `finagle-memcached`: KetamaClient based on finagle-6 API
- `finagle-exception`: unique namespace for scribe
- `finagle-thrift`: Add scrooge3 support
- `finagle`: bump sbt util/finagle version
- `finagle`: Updating owner list
- `finagle`: Upgrade util to 6.3.6 and scrooge-runtime to 3.1.2

6.4.1
-----

- `finagle-http`: CookieMap instead of CookieSet
- `finagle-memcached`: remove items stats test since twemcache does not carry those
- `finagle-http`: don't choke on HEAD requests
- `finagle-core`: ReusingPool: fix race between setting Future.never and initiating connect
- `finagle-thriftmux`: also release services
- `finagle-http`: Use the correct dispatcher for RichHttp and new API
- `finagle-exception`: a loadable exception reporter
- `finagle-core`: make global flags consistently camelCase
- `finagel-http`: enable tracing by default for Http
- `finagle-http`: Call super method only if response is not chunked
- `finagle-core`: Tracing changes to annotate both client and server ip addresses
- `finagle-http`: Stream interface (i.e. chunked) for HTTP message bodies
- `finagle-core`: get a resolver instance contained in the main Resolver
- `finagle-mux`: fix TagMap.iterator
- `finagle-mdns`: Adding project to sbt
- `inagle`: Fix doc
- `finagle-mysql`: refactor endec into proper fsm
- `finagle`: Update sbt project for (util, ostrich, finagle)

6.4.0
-----

- `finagle-core`: Add unmanaged cache pool cluster
- `finagle-core`: Always enable TracingFilter even when no tracers are used.
- `finagle-core`: cache the cancelled connections in WatermarkPool instead of CachingPool
- `finagle-core`: Fix behavior of proxy forwarding in StatsReceiver
- `finagle-core`: Fix usage of RollupStatsReceiver in StatsFactoryWrapper.
- `finagle-core`: Generalize LoggingFilter
- `finagle-core`: HeapBalancer: fix handling of unavailable nodes
- `finagle-core`: Properly synchronize concurrent access to ProxyService state.
- `finagle-core`: Refactor Heapbalancer, remove usage of per host statsReceiver.
- `finagle-core`: share worker pools between server and client
- `finagle-exception`: Fix sourceHost supplied to chickadee exception reporter
- `finagle-http`: Add HttpConnectHandler and HttpClientCodec in the correct order when setting up an HTTP proxy client.
- `finagle-http`: add Response(status)
- `finagle-http`: EncodeBytes/decodeBytes for Request
- `finagle-http`: Refactor HeaderMap/ParamMap
- `finagle-memcache`: change to use consistent way to provide failure accrual params
- `finagle-memcached`: Propagating error message from server to client
- `finagle-memcached`: support for configuring numReps in KetamaClientBuilder
- `finagle-mux`: convert tests to use ScalaTest
- `finagle-mysql`: Ability to deal with non-ascii utf-8 characters
- `finagle-mysql`: Add ability to pass StatsReceiver to builder
- `finagle-mysql`: adds support for latin1_bin, paves the way for more charsets.
- `finagle-mysql`: Encode all the given the error information in the ServerError
- `finagle-mysql`: Fix improper use of ServiceFactory in Client
- `finagle-redis`: Fix byte encoding problem with Z(Rev)Rank
- `finagle-redis`: fixing Nil values in MBulkReplies
- `Finagle-zipkin`: decoupled sampling logic from ZipkinTracer to make a reusable SamplingTracer
- `finagle-zipkin`: Improve zipkin trace sampling
- `finagle-zipkin`: make ZipkinTracer loadable
- `finagle`: Document metrics
- `finagle`: Force service loader to return a concrete collection
- `finagle`: Refactor Future#get to Await.result
- `finagle`: Upgrade jackson to 1.9.11
- `finagle`: Upgrade ostrich to 9.1.1
- `finagle`: Upgrade util to 6.3.4

6.3.0
-----

- `finagle`: Upgrade util to 6.3.0
- `finagle-thrift`: SocketAddress when serving an iface
- `finagle-core`: Specify DefaultTimer when creating a ChannelFactory
- `finagle-core`: DefaultClient/Server: Do not add TracingFilter if NullTracer is used

6.1.4
-----

- `finagle-zipkin`: tracing improvements
- `finagle`: Upgrade util to 6.2.5

6.1.3
-----

- `finagle-core`: Add BroadcastCounter and BroadcastStat, refactor Broadcast and Rollup Receiver.
- `finagle`: update sbt, add finagle-exp
- `finagle-doc`: Add a FAQ, add an entry about CancelledRequestExceptions
- `finagle-core`: Add client support for proxies that support HTTP CONNECT, such as squid.
- `finagle-doc`: A few wording fixes in Sphinx doc templates.
- `finagle-core`: StatsFilter: Check for WriteException-wrapped backup-request losers
- `finagle-mysql`: bug fixes
- `finagle`: Update sbt project definition
- `finagle-core`: Introduce DefaultTracer that load available Tracers via LoadService + Fix equality with NullStatsReceiver
- `finagle-core`: CachingPool: Do not schedule timer tasks if ttl is Duration.Top

6.1.2
-----

Released 2013/03/21

- `finagle`: Fix flakey tests
- `finagle`: fix sbt build
- `finagle`: Upgrade util to 6.2.4
- `finagle-core`: Fix reporting bug for pending reqs
- `finagle-core`: Move Disposable/Managed to util
- `finagle-core`: Use Future.Nil
- `finagle-doc`: Improve, add Matt Ho's talk.
- `finagle-exp`: Add BackupRequestFilter, new backup request filter using response latency quantiles
- `finagle-http`: Avoid URISyntaxException
- `finagle-memcached`: Add Replication Cache Client
- `finagle-mysql`: Make prepareAndExecute and prepareAndSelect useful again.
- `finagle-mysql`: Make sure we are properly managing prepared statement
- `finagle-mysql`: Move interfaces outside of protocol package
- `finagle-redis`: Moved redis server classes from private to public
- `finagle-thrift`: fix 2.10 build

6.1.1
-----

Released 2013/03/12

- `doc`: Finagle user guide!
- `finagle`: Remove finagle's dependence on jerkson.
- `finagle`: Upgrade Netty to 3.5.12
- `finagle`: Upgrade scrooge-runtime to 2.4.0
- `finagle`: Upgrade util to 6.2.2
- `finagle-core`: com.twitter.finagle.Group: introduce names, use them.
- `finagle-core`: deprivatize, document Client, Server.
- `finagle-core`: fix multiple stats bugs.
- `finagle-core`: Log Finagle's version number on startup.
- `finagle-core`: RetryingFilter, don't retry on CancelledRequestException.
- `finagle-http`: New Request object queryString utility functions.
- `finagle-http`: New Request/Response encode/decode functions.
- `finagle-memcached`: Actively reestablish zk connection whenever disconnected or expired.
- `finagle-memcached`: Cache pool cluster not updated since 2nd serverset change.
- `finagle-memcached`: Don't use deprecated FuturePool.defaultPool.
- `finagle-mysql`: Add project in the list of deployed jar.
- `finagle-mysql`: Fix race condition in the authentication process.
- `finagle-mysql`: Move it to experimental package.
- `finagle-ostrich4`: Add OstrichExporter to enable Ostrich stats in the new App stack.
- `finagle-redis`: Add support for INFO command.
- `finagle-resolver`: additional visibility for Announcers and Resolvers.
- `finagle-zipkin`: new factory methods.

6.1.0
-----

Released 2013/01/30

- `finagle`: update util and ostrich dependencies
- `finagle`: libraries: preliminary 2.10 port/build
- `finagle`: Tracer use new daemon() flag; get rid of factory. Simplify.
- `finagle`: refactor clients and servers to be simpler, more logical.
- `finagle-core`: Provide a reportHostStatsTo API on ClientBuilder that per-host stats will be reported to.
- `finagle-core`: Use Groups in place of Clusters
- `finagle-core`: Create a NonShrinkingCluster
- `finagle-redis`: made zRange... commands work more like the actual redis commands
- `finagle-stats`: new project that provides a StatsReceiver based on Twitter's Metrics library.
- `finagle-http`: add new-style client and server, including a primitive URL fetcher

6.0.5
-----

Released 2013/01/22

- `finagle-core`: basic SOCKS support for finagle clients, intended for use with ssh -D.

6.0.4
-----

Released 2013/01/22

- `finagle-core`: CachingPool owns connections and caches them even if upstream cancels
- `finagle-core`: Eliminate varargs overhead in StatsReceiver.time and timeFuture
- `finagle-core`: LoadBalancerTest: allow the tests to be run with Google Caliper
- `finagle-core`: make snap on mapped cluster thread-safe
- `finagle-memcached`: explicitly pass in initial cache nodes when using a a static cluster
- `finagle-native`: fix up grab_and_path_tomcat_native
- `finagle-redis`: finagle redis btree sorted set commands
- `finagle-redis`: Implemented redis HMSET
- `finagle-redis`: expireAt command

6.0.3
-----

Released 2012/12/18

- `finagle-mux`: new session protocol
- `documentation`: document connection management and associated configuration options
- `finagle-core`: expose newChannelTransport in Codec to allow multiplexing in clients
- `finagle-http RequestProxy`: proxy response

6.0.2
-----

Released 2012/12/11

- `TimerSpec, RefcountedSpec`: make deterministic
- `finagle-core`: JSSE SSL support for intermediate certificates
- `finagle-core`: push SSLEngine all the way up to the ServerBuilder
- `finagle-core`: preclude default SourceMonitor in clients
- `finagle-core`: ReusingPool for pipelined, multiplexed clients
- `finagle-core`: remove wasteful RichLong allocations in hot path
- `finagle-core`: move acquire/release of services outside of WatermarkPool's synchronization
- `finagle-core`: remove unintended reflection-by-refinement-type
- `finagle-thrift`: actually support multiple protocols
- `finagle-ostrich4`: configurable delimiter
- `finagle-redis`: parse case-insensitive commands

6.0.1
-----

Released 2012/11/26

- Use java.util.ArrayDeque in place of mutable.Queue due to https://issues.scala-lang.org/browse/SI-6690

6.0.0
-----

Released 2012/11/26

- util 6.0.0 dependency
- `finagle-core`: new client/server APIs; not yet exposed to users

5.3.23
------

Released 2012/11/21

- `finagle-spdy`: new codec
- `finagle-core`: ServiceTimeoutException is now a WriteException (so they can be retried)
- `finagle-core`: close channels owned by ChannelTransport before failing queues
- `finagle-core`: share ChannelStatsHandler in clients and servers to avoid excessive stats-related weakrefs
- `finagle-core`: don't leak Spooled updates in the HeapBalancer

5.3.22
------

Released 2012/11/08

- `finagle-redis`: String commands
- `finagle-redis`: collection.Set => collection.immutable.Set per #116
- `finagle-thrift`: SeqIdFilter: Don't modify thrift requests.
- `BackupRequestFilter`: experimental service filter to perform backup requests

5.3.20
------

Released 2012/10/26

- `finagle-redis`: fix zrevrangebyscore (https://github.com/twitter/finagle/issues/114)
- `finagle-memcached`: fix space leak when using clustering
- `finagle-core`: special case 1-hosted (static) clients to bypass loadbalancer
- `finagle-core`: Introduce an experimental buffer-based pool

5.3.19
------

Released 2012/10/16

- Upgrade ostrich to 8.2.9
- Upgrade util to 5.3.13
- Upgrading slf4j to 1.6.1
- `finagle`: Rm usage of deprecated Predef.error.
- `finagle-core`: Changed stats name in ChannelStatsHandler from exception msg to getClass.getName
- `finagle-core`: Add counters to track disconnections due to Idle activity (both read/write)
- `finagle-core`: Add "adds"/"removes" counters in HeapBalancer to track modifications
- `finagle-core`: Added name to HeapBalancer init (via exception) if HB size is 0
- `finagle-core`: Don't log dropped events: they are actually quite numerous.
- `finagle-kestrel`: Disable failFast for kestrel.
- `finagle-memcached`: Cluster support within finagle-memcached (phase 2)
- `finagle-redis`: added new commands

5.3.18
------

Released 2012/09/27

- `Doc`: update README with working thrift Java example
- `Doc`: fix compile errors in examples
- `finagle-http`: fix typo in RequestBuilder API
- `finagle-stress`: FrontEndServerStress refactor; now generates runnable binary
- `finagle-core`: add optional `shouldRetry` argument to RetryPolicy.tries
- `finagle-core`: pools propagate underlying availability correctly
- `finagle-core`: move fail-fast before the pool. This prevents it from masking errors from the failure accrual mechanism, and is arguably the "correct" placement
- `finagle-core`: fix argument type in RetryingFilter.apply
- `finagle-core`: failFast by default; disable for Kestrel
- `finagle-memcached`: cache-pool specific Cluster implementation
- `finagle-memcached`: add stat to cache-pool cluster
- `finagle-redis`: add scan, hscan commands
- `finagle-redis`: add list commands

5.3.9
-----

Released 2012/09/07

- `finagle`: Update/fix README
- `finagle`: Upgrade util to 5.3.10
- `finagle`: Update guava to v13
- `finagle-redis`: API v2 + update example
- `finagle-thrift`: validate connections based on TApplicationException
- `finagle-core`: SSL: fix connection failure propagation in futures

5.3.8
-----

2012/08/24


- `finagle-core`: Upgrade to Netty 3.5.5

5.3.7
-----

Released 2012/08/24

- `Doc`: Fix mistake in finagLe README that claimed Future objects have a getContentAsync method
- `Doc`: Fix typos in README
- `finagle-thrift`: ThriftChannelBufferDecoder: fix for composite buffers
- `finagle-mysql`: MySQL codec from Ruben Oanta.
- `finagle-core`: add isSuccess to FailureAccrualFactory
- `finagle-http`: Fix file extension bug in finagle-http

5.3.6
-----

Released 2012/06/17

- `Doc`: Typos
- `finagle-core`: fix canceling recurring timers
- `finagle`: kill finagle-memcached-hadoop
- `finagle-memcached`: in-process memcache server now supports expiry
- `finagle-memcached`: Adding key length validation and refactor validation logic.

5.3.5
-----

Released 2012/08/10

- `finagle-core`: Fixed stats visibility
- `finagle-serversets`: allow specification of an additional endpoint name in ZookeeperServerSetCluster
- `finagle-serversets`: ZookeeperServerSetCluster.join now return an EndpointStatus + Named/Daemonized init thread
- `finagle-core`: Changed Cache's internal implementation from queue to deque (to allow LIFO behavior)
- `finagle-core`: cluster initialization honors global timeout in ClientBuilder.
- `finagle-stress`: finagle: kill obsolete benchmark

5.3.4
-----

Released 2012/08/02

- `finagle`: patch public release of OSS libraries; catch up sbt
- `finagle-core`: netty 3.5.3.Final
- `finagle-core`: Timer: address review comments, fix tiny race

5.3.3
-----

Released 2012/07/31

- `finagle-core`: Timer: dispose timeouts on cancellation

5.3.2
-----

Released 2012/07/31

- `finagle-core`: Fix critical bug in Timer by refactoring the Timer model
- `finagle`: Add OWNERS file to finagle-memcached, finagle-redis
- `finagle-redis`: add keys and hkeys
- `finagle-zipkin`: Add a debug flag, add an i64 bit field to the thrift request header struct and as a http header
- `finagle-core`: SourceTrackingMonitor to report whether it's in client or server
- `finagle-zipkin`: Add duration to the the JVM gc annotation.

5.3.1
-----

Released 2012/07/20

- `finagle-serversets`: Use authenticated zookeeper client suggested for new SD cluster migration
- `finagle-thrift`: seqid filter: improved tests
- `finagle-zipkin`: Add duration to annotations for timing operations
- `finagle-serversets`: bump up server-set version to 1.0.10 (latest) in finagle-serversets

5.3.0
-----

Released 2012/06/25

- `finagle-core`: ClientBuilder and ServerBuilder's MonitorFilter to report exception sources
- `finagle-test`: Add TaskTrackingTimer benchmark
- `finagle-thrift`: SeqIdFilter: helpful exceptions
- `Dispatcher`: all write exceptions are wrapped with WriteException

5.1.0
-----

Released 2012/06/14


- `finagle`: Upgrade util to 5.2.0, ostrich to 8.1.0
- `finagle-redis`: extend codec
- `finagle-memcache`: first phase of serverset support
- `finagle-b3`: we are using zipkin now
- `finagle-kestrel`: Kill com.twitter.concurrent.Channel
- `finagle-core`: JvmFiler, trace jvm events

5.0.2
-----

Released 2012/06/01


- `finagle-core`: Refactor the new way of managing resources
- `finagle-core`: Add capability to drain dispatcher on expire
- `finagle-core`: ZookeeperServersetCluster notify client when underlying Cluster is ready
- `finagle-exception`: Upgrade jerkson to 0.5.0 and streamyj to 0.4.1

5.0.0
-----

Released 2012/05/25


- Upgrade to scala 2.9.2
- `finagle-core`: FailFast, do not admit requests when marked failed
- `finagle-redis`: now return tuples and doubles
- `finagle-memcached`: record client recv annotation after hit/miss
- `finagle-core`: expand exception chain in StatsFilter

4.0.0
-----

Released 2012/05/08


- `http`: add AddResponseHeadersFilter, CorsFilter
- `finagle-redis`: Add ZCard, ZRem, ZRevRange, ZRevRangeByScore commands
- `finagle-memcached`: Added b3 annotations for key hits/misses
- `finagle-memcached`: getResult/getsResult for java
- `finagle-http`: fix client codec
- `HeapBalancer`: close services that are removed from cluster
- `finagle-redis`: add tracing and stats support
- `finagle-http`: Add the client address for http traces. Also record uri after service name has been set
- `finagle-memcached`: more efficient implementation of GetResult.merged
- `finagle-redis`: return pairs for hgetall, zrangebyscores, zrevrangebyscores cmds
- `finagle-core`: introduce Transports and Dispatchers
- `tracing`: annotate client connection when tracing
- update to netty 3.4.1.Final
- prevent tenuring of response objects for responses from previously idle connections
- `finagle-redis`: add quit command
- `finagle-http`: add service multiplexer
- friendlier java interface for retries

3.0.0
-----

Released 2012/03/14


- `ServiceToChannelHandler`: keep track of shutdowns with pending replies, don't monitor errors that occur after cancellation.
- Add two received_bytesm sent_bytes to per-channel stats.
- `http`: Add wwwAuthenticate(=) and isXmlHttpRequest
- `http`: CancelledRequestExceptions become 499s
- `clients`: experimental Fail-Fast mode (ClientBuilder.expFailFast(true))
- `b3`: Only log that a span was unfinished if we actually found one
- `thrift`: better support for one-way calls
- `server`: option for cancellation on hangup (default=true)
- `codecs`: improved support for multiplexed protocols
- upgrade to netty 3.4.0.alpha1 which fixes connect timer resolution issues. Trustin says this should be as stable as 3.3.1.Final
- `serversets`: allow for additional endpoints
- `tests`: use true ephemeral ports instead of RandomSocket
- `memcache stress`: use new command line flag library from commons
- `thrift`: concatenate arrays with System.arraycopy
- `memached`: fix bug wherein we made requests with empty keys on retries

2.0.1
-----

Released 2012/03/01


- `commons-stats`: memoize counters
- `cluster`: don't reuse adds; keep many values per key
- `clientbuilder`: buffer handlers to avoid deadlock

2.0.0
-----

Released 2012/02/27


- unify client and server factories
- `memcache stress test`: one connection per process
- `b3`: make tests deterministic
- upgrade to newest util (and to Guava r11)

1.11.1
------

Released 2012/02/15


- `HeapBalancer`: safely remove services from cluster when they have requests in flight
- `http`: speed up logging
- `core`: keep track of handler walltimes

1.11.0
------

Released 2012/02/13


- `finagle-redis`: publish.
- `http`: Message.mediaType=: no longer sets charset to utf-8
- `http`: Message.setContentType: defaults charset to utf-8
- `http`: added Message.charset, Message.charset=
- Refactor resource management inside of the builders, making lifetimes explicit
- Generic cluster API, support in the HeapBalancer for dynamic hosts list; remove other load balancers
- `redis`: memcache.Client-style interface
- `netty`: update to 3.3.1.Final, and add NPN TLS extension support
- `b3`: flush span if timeout occurs
- `b3`: java friendly tracer API
- `stress/example`: "topo", a minimal appserver-like topology for testing
- `memcache`: tunable example for benchmarking, testing
- `ServerBuilder`: major refactor & simplification
- `SummarizingStatsReceiver`: fix bug where sort didn't operate over a stable collection

1.10.0
------

Released 2012/01/24


- `http`: Message.withX methods now have parameterized return types
- `memcached`: (Ketama) host ejection
- Noisier, more robust monitoring.
- `zk cluster`: avoid potential infinite loop
- `http streaming`: better detection, handling of dead channels
- `http`: always encode output as UTF-8
- `stream`: use offer/broker for duplex stream
- `redis`: imported finagle redis client from Tumblr. not yet published.

1.9.12
------

Released 2012/01/05


- `SSLEngine`: do not reuse between connections
- `ServerBuilder.Server`: localAddress to get ephemeral port values
- More detailed client stats
- `kestrel`: parse parseStatsLines
- `FailFastFactory`: new fast-fail strategy with maxOutstandingConnections
- `exceptions`: add peer information to connection failures, add service name to all exceptions
- `fixes`: space leak in stats, ReliableDuplexFilter lazy load ordering
- `b3`: allow the user to specify sample rate
- `exception reporter`: share scribe connection across instances
- `finagle-ostrich`: remove, it's obsoleted by finagle-ostrich4

1.9.10
------

Released 2011/12/02

- update to util 1.12.7

1.9.9
-----

Released 2011/12/01

- update to util 1.12.6

1.9.8
-----

Released 2011/11/30

- upgrade to netty 3.2.7.Final
- `streaming`: support HTTP/1.0
- `native`: fix content-length header

1.9.7
-----

Released 2011/11/29

- `http`: Propagate CancelledRequestException rather than continuing
- `thrift`: deprecated LookupContext, using finagle.thrift.ClientId
- use monitors where applicable (composable widgets for handling exceptions)
- `memcache`: PHPMemCacheClient
- make RetryingFilter usable from Java
- `thrift`: convert uncaught application exceptions into TApplicationExceptions on the wire
- NoStacktrace.getStackTrace() returns non-empty stacktrace array
- kill off openConnectionsHealthThresholds (unused)

1.9.6
-----

Released 2011/11/08

- `SSL`: make native provider bootstrapping code work; refactor 'Ssl' singleton
- `tracing`: Update configurations and libraries to use tracer factory introduced to make it possible to release resources properly in tracers
- `finagle-thrift`: connection options + client id
- a more flexible retry filter

1.9.5
-----

Released 2011/11/04

- `ExpiringService`: add stats for idle and lifetime expiration. Deactivate() regardless of latch counter
- `Filters`: don't apply refcounts on composition, do it only (and always) in the builder
- Count pending timeouts, and export them
- `ChannelService`: release services when `prepareChannel` fails. This fixes a connection leak
- `WatermarkPool`: flush waiters on creation failure. This fixes a condition wherein we'd keep waiters unnecessarily.
- `Timeout exceptions`: include service name
- Suppress excessive logging of the exceptions that mostly caused by unreliable client connections.
- `memcached`: Add generic type support (via Client.adapt), and Array[Byte] and String instances
- `finagle-stream`: do not admit concurrent requests
- `Tracing`: Add release method to Tracer trait, add tracerFactory to builders, deprecate tracer on builders

1.9.4
-----

Released 2011/10/24

- `Service`: response Future will now be cancelled if the client disconnects
- use static exceptions for TimedOutRequestException
- `kestrel`: Cancel request Future on close in kestrel ReadHandle
- `exception cleanup`: wrap all Channel exceptions with their underlying exception. This makes UnknownChannelExceptions in particular easier to debug
- `Friendlier exceptions`: In order to give very specific error messages to timeouts in finagle (so beginners know which settings in the client and server builder caused them), I'm adding lots of human-readable messages to the various TimeoutExceptions.
- `some timer bug fixes, particularly`: TimerToNettyTimer: actually cancel underlying task.

1.9.2
-----

Released 2011/10/14

- `tracing`: Change SpanId to use RichU64String
- `tracing`: Trace.{enable,disable}
- `http`: http.RequestBuilder
- `http`: add common media types

1.9.1
-----

Released 2011/09/28

- `memcached`: new interface for partial results (ie. retrieval failures + successes)
- `tracing`: set service name in the endpoint instead of the span
- more idiomatic KetamaClientBuilder constructor
- `tracing`: convenience methods to add binary annotations
- `finagle-http`: misc. fixes
- HeapBalancer - a new lg n load balancer (asymptotically more efficient!)
- upgrade to ostrich 4.9.0

1.9.0
-----

Released 2011/08/29

- `http`: new http library

1.8.4
-----

Released 2011/08/23

- `tracing`: fixed space leak
- `ssl`: integrate openssl sslengine
- `thrift`: update to sbt-thrift 2.0.1
- `stream`: always release server when we close
- `serversets`: upgraded to guava-r09
- `core`: expose stats on failed retries

1.8.3
-----

Released 2011/08/12

- `thrift`: add a server-side buffered codec; fix tracing issue in buffered codec
- `http`: workaround hole in netty logic to guarantee maxRequestSize
- `serversets`: don't block on ClientBuilder construction
- `ostrich4`: update to ostrich 4.8.2
- `streaming`: reimplement HTTP chunked streaming in terms of offer/broker
- `kestrel`: buffered & merging of ReadHandles

1.8.1
-----

Released 2011/08/05

- upgrade to netty 3.2.5.Final
- `kestrel`: fix visibility of ResultWithCAS
- `kestrel`: Client.{read,write}, Client.readReliably, MultiReader: suite of kestrel streaming readers (including grabby-hands replacements) using Offer/Broker
- `kestrel`: don't render timeouts > 2^31-1
- `tracing`: fix sampling bug
- `tracing`: trace the kestrel client
- quench unecessary error reporting

1.8.0
-----

Released 2011/08/02

- `SSL`: perform hostname validation in the client. this is the reason for the minor bump: we change the .tls() ClientBuilder signature
- introduce util-codec dependency for use of the apache commons base 64 codec

1.7.5
-----

Released 2011/07/29


- make it easier to use the NullStatsReceiver from java
- convert WriteException to a case class (github issue 25)
- add _root_. to java.util.Map import. reorder imports
- Upgrade finagle-common-stats to twitter-common-stats 0.0.16
- encode more HTTP codec errors
- adding cardinality to ServiceException
- `finagle CachingPool`: limit size to highWatermark - lowWatermark
- `exception reporting`: gzipping trace strings
- set ostrich version of finagle-ostrich4 to 4.7.3
- `ChannelServiceFactory`: encode exceptions from bootstrap.connect()

1.7.4
-----

Released 2011/07/22


- `http`: convert codec exceptions into 4xx errors; report them as such upstream
- `memcache`: `gets` and `cas` support.

1.7.3
-----

Released 2011/07/19


- `fix`: 1.7.2 introduced a regression in the pool that may, under certain circumstances, admit connections beyond the high watermark.
- quench exception for setting multiple Promise values when racing with write errors.
- Adds tracing support for the Memcached client. This only creates the client side annotations, no support for passing along ids and such to the memcached server.


1.7.2
-----

Released 2011/07/18


- support end-to-end cancellation in finagle clients; fixes possible pool starvation conditions, and allows clients to cancel requests and reclaim resources
- `end-to-end timeouts`: introduce, .connectTimeout and .timeout. .connectTimeout is the end-to-end connection timeout (includes acquisition & TCP time), .timeout is an end-to-end request time: no requests will take longer
- some doc fixes
- misc. bug fixes
