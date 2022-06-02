Flags
=====

Finagle features that cannot be turned on/off on a per client/server basis are usually controlled
via global command line flags. This document lists such flags along with their default values.

Finagle's global flags could be either passed as Java application flags (when run within
Twitter Server) or as JVM properties.

.. code-block:: scala

  -com.twitter.finagle.foo=bar  // Java application argument
  -Dcom.twitter.finagle.foo=bar // JVM arg/property

Common
------

**com.twitter.finagle.client.useNackAdmissionFilter** `bool`
  A global on/off switch for client-side nack admission control (default: `true`,
  which opts clients into using nack admission control). This controller is designed
  to allow backends to recover when they overloaded. See
  `com.twitter.finagle.filter.NackAdmissionFilter` for more details. This value can
  also be set on per-client with `$Client.configured(NackAdmissionFilter.Param)`.

**com.twitter.finagle.loadbalancer.defaultBalancer** `choice|heap|aperture|random_aperture`
  The default load balancer used in clients (default: `choice`). `random_aperture` should only
  be used in situations where subsetting is a firm requirement. An example of that is in a testing
  situation where p2c behavior isn't acceptable. In other cases `aperture` will select between
  random aperture and deterministic aperture when appropriate.

**com.twitter.finagle.loadbalancer.perHostStats** `bool`
  Enable/disable per-host granularity for stats (default: `false`). When enabled,the configured stats
  receiver will be used, or the loaded stats receiver if none given.

**com.twitter.finagle.loadbalancer.aperture.minDeterministicAperture** `int`
  Set the lower bound of the aperture size when using the determistic aperture load balancer (default: 12).

**com.twitter.finagle.socks.socksProxyHost** `string`
  When non empty, enables SOCKS proxy on each Finagle client (default: empty string).

**com.twitter.finagle.socks.socksProxyPort** `int`
  A port number for a SOCKS proxy (default: `0`).

**com.twitter.finagle.socks.socksUsername** `string`
  A username for a SOCKS proxy (default: empty string).

**com.twitter.finagle.socks.socksPassword** `string`
  A cleartext password for a SOCKS proxy (default: empty string).

**com.twitter.finagle.tracing.enabled** `bool`
  When false, disables any tracing for this process (default: `true`). Note: it's never recommended
  to disable tracing in production applications.

**com.twitter.finagle.tracing.traceId128Bit** `bool`
  When true, new root spans will have 128-bit trace IDs (default: `false`, 64-bit IDs).

**com.twitter.finagle.util.defaultTimerProbeSlowTasks** `bool`
  Enable reporting of slow timer tasks executing in the default timer (default: `false`).

**com.twitter.finagle.util.defaultTimerSlowTaskMaxRuntime** `duration`
  Maximum runtime allowed for tasks before they are reported (default: `2.seconds`).

**com.twitter.finagle.util.defaultTimerSlowTaskLogMinInterval** `duration`
  Minimum interval between recording stack traces for slow tasks (default: `20.seconds`).

**com.twitter.finagle.offload.numWorkers** `int`
  Experimental flag. Enables the offload filter using a thread pool with the specified number of threads.
  When this flag is greater than zero, the execution of application code happens in an isolated pool and the netty threads are used only to handle the network channels. This behavior changes the assumptions regarding the scheduling of tasks in finagle applications. Traditionally, the recommendation is to execute CPU-intensive tasks using a `FuturePool` but, when this flag is enabled, CPU-intensive tasks don't require a `FuturePool`. Important: Blocking tasks should still use a `FuturePool`.
  It's important to review the allocation of thread pools when this flag is enabled otherwise the application might create too many threads, which leads to more GC pressure and increases the risk of CPU throttling.

**com.twitter.finagle.offload.queueSize** `int`
  Experimental flag. When offload filter is enabled, its queue is bounded by this value (default:
  "unbounded" or `Int.MaxValue`) Any excess work that can't be offloaded due to the queue overflow
  is run on IO (Netty) threads instead. Thus, when set, this flag enforces the backpressure on the
  link between "Netty (producer) and your application (consumer).

**com.twitter.finagle.offload.statsSampleInterval** `duration`
  When offload filter is enabled, sample additional offload queue stats (`delays_ms`) at this
  interval (default: `100.milliseconds`). Only finite and positive values are accepted, everything
  else disables the stats.

**com.twitter.finagle.offload.auto** `bool`
  Experimental flag.
  When enabled the offload filter will be enabled and the worker pool sizes of both the application
  pool and underlying I/O system will be set to reasonable values (default: `false`).

**com.twitter.finagle.offload.admissionControl** `none|default|enabled|duration`
  Experimental flag.
  When this flag is used in conjunction with flags that enable the offload filter it will also
  enable offload-based admission control. Offload admission control is based on the worker pools
  work queue latency, an estimation of how long a task is expected to wait in the work queue before
  it will be processed. When using `enabled` a reasonable default will be used for the maximum
  acceptable work queue delay. Tuning can be accomplished by setting the flag to a duration, for
  example, `50.milliseconds`. A value of `default` currently has the same meaning as `none`, which
  is to disable offload admission control.

Netty 4
-------

**com.twitter.finagle.netty4.numWorkers** `int`
  The number of Netty 4 worker threads in the shared even pool (default: `CPUs * 2`)

**com.twitter.finagle.netty4.trackReferenceLeaks** `bool`
  Enable reference leak tracking in Netty 4 and export a counter at `finagle/netty4/reference_leaks`
  (default: `false`).

**com.twitter.finagle.netty4.timerTicksPerWheel** `int`
  Netty 4 timer ticks per wheel (default: `512`).

**com.twitter.finagle.netty4.timerTickDuration** `duration`
  Netty 4 timer tick duration (default: `10.milliseconds`).

**com.twitter.finagle.netty4.useNativeEpoll** `bool`
  When available, use Linux's native epoll transport directly instead of bouncing through JDK
  (default: `true`).

**com.twitter.finagle.netty4.http.revalidateInboundHeaders** `bool`
  Validate headers when converting from Netty to Finagle. (default: `false`).

Stats
-----

**com.twitter.finagle.stats.statsFilter** `string`
  Comma-separated list of regexes that indicate which metrics to filter out (default: empty string).

**com.twitter.finagle.stats.statsFilterFile** `list of files`
  Comma-separated list of files of newline-separated regexes that indicate which metrics to filter
  out (default: empty list).

**com.twitter.finagle.stats.useCounterDeltas** `bool`
  When true, export deltas for counters instead of absolute values (default: `false`).

**com.twitter.finagle.stats.debugLoggedStatNames** `list of strings`
  Comma-separated stat names for logging observed values (default: empty list).

**com.twitter.finagle.stats.scopeSeparator** `string`
  Override the default scope separator (default: `/`).

**com.twitter.finagle.stats.format** `commonsmetrics|commonsstats|ostrich`
  Format style for stat names (default: `commonmetrics`).

**com.twitter.finagle.stats.includeEmptyHistograms** `bool`
  Include full histogram details when there are no data points (default: `false`).

**com.twitter.finagle.stats.verbose** `string`
  Comma-separated list of *-wildcard expressions to allowlist debug metrics that are not exported by
  default (default: undefined). A tunable, `com.twitter.finagle.stats.verbose` has a higher priority
  if defined.

Http
----

**com.twitter.finagle.http.serverErrorsAsFailures** `bool`
  Treat responses with status codes in the 500s as failures (default: `true`).
