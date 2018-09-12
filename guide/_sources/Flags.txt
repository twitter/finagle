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

**com.twitter.finagle.loadbalancer.defaultBalancer** `choice|heap|aperture`
  The default load balancer used in clients (default: `choice`).

**com.twitter.finagle.loadbalancer.perHostStats** `bool`
  Enable/disable per-host granularity for stats (default: `false`). When enabled,the configured stats
  receiver will be used, or the loaded stats receiver if none given.

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

Netty 4
-------

**com.twitter.finagle.netty4.numWorkers** `int`
  The number of Netty 4 worker threads in the shared even pool (default: `CPUs * 2`)

**com.twitter.finagle.netty4.trackReferenceLeaks** `bool`
  Enable reference leak tracking in Netty 4 and export a counter at `finagle/netty4/reference_leaks`
  (default: `true`).

**com.twitter.finagle.netty4.timerTicksPerWheel** `int`
  Netty 4 timer ticks per wheel (default: `512`).

**com.twitter.finagle.netty4.timerTickDuration** `duration`
  Netty 4 timer tick duration (default: `10.milliseconds`).

**com.twitter.finagle.netty4.useNativeEpoll** `bool`
  When available, use Linux's native epoll transport directly instead of bouncing through JDK
  (default: `true`).

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

Http
----

**com.twitter.finagle.http.serverErrorsAsFailures** `bool`
  Treat responses with status codes in the 500s as failures (default: `true`).
