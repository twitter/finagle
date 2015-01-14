package com.twitter.finagle;

import java.net.SocketAddress;

import scala.Option;

import com.twitter.finagle.channel.IdleConnectionFilter;
import com.twitter.finagle.channel.OpenConnectionsThresholds;
import com.twitter.finagle.client.DefaultPool;
import com.twitter.finagle.client.Transporter;
import com.twitter.finagle.factory.BindingFactory;
import com.twitter.finagle.factory.TimeoutFactory;
import com.twitter.finagle.filter.MaskCancelFilter;
import com.twitter.finagle.filter.RequestSemaphoreFilter;
import com.twitter.finagle.loadbalancer.LoadBalancerFactory;
import com.twitter.finagle.netty3.Netty3Transporter;
import com.twitter.finagle.netty3.param.Netty3Timer;
import com.twitter.finagle.param.Label;
import com.twitter.finagle.param.Logger;
import com.twitter.finagle.param.Monitor;
import com.twitter.finagle.param.Reporter;
import com.twitter.finagle.param.Stats;
import com.twitter.finagle.param.Timer;
import com.twitter.finagle.param.Tracer;
import com.twitter.finagle.server.Listener;
import com.twitter.finagle.service.ExpiringService;
import com.twitter.finagle.service.FailFastFactory;
import com.twitter.finagle.service.FailureAccrualFactory;
import com.twitter.finagle.service.TimeoutFilter;
import com.twitter.finagle.socks.SocksProxyFlags;
import com.twitter.finagle.ssl.Engine;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.transport.Transport;
import com.twitter.util.Duration;
import com.twitter.util.Function0;

public class StackParamCompilationTest {
  void testParams() {
    Label label = new Label("");
    Timer timer = new Timer(com.twitter.finagle.util.DefaultTimer.twitter());
    Logger logger = new Logger(java.util.logging.Logger.getLogger("com.twitter.finagle"));
    Stats stats = new Stats(com.twitter.finagle.stats.DefaultStatsReceiver.get());
    Monitor monitor = new Monitor(com.twitter.finagle.util.DefaultMonitor.get());
    Reporter reporter = new Reporter(com.twitter.finagle.util.LoadedReporterFactory.get());
    Tracer tracer = new Tracer(com.twitter.finagle.tracing.DefaultTracer.get());
    FactoryToService.Enabled enabled = new FactoryToService.Enabled(true);
    IdleConnectionFilter.Param idleConnectionFilterParam =
      new IdleConnectionFilter.Param(Option.<OpenConnectionsThresholds>empty());
    DefaultPool.Param defaultPoolParam =
      new DefaultPool.Param(0, Integer.MAX_VALUE, 0, Duration.Top(), Integer.MAX_VALUE);
    Transporter.ConnectTimeout transporterConnectTimeout =
      new Transporter.ConnectTimeout(Duration.Top());
    Transporter.TLSHostname transporterTLSHostname =
      new Transporter.TLSHostname(Option.<String>empty());
    Transporter.SocksProxy transporterSocksProxy =
      new Transporter.SocksProxy(
        SocksProxyFlags.socksProxy(),
        SocksProxyFlags.socksUsernameAndPassword());
    Transporter.HttpProxy transporterHttpProxy =
      new Transporter.HttpProxy(Option.<SocketAddress>empty());
    BindingFactory.BaseDtab bindingFactoryBaseDtab =
      new BindingFactory.BaseDtab(new Function0<Dtab>() {
          public Dtab apply() { return Dtab.empty(); }
        });
    TimeoutFactory.Param timeoutFactoryParam = new TimeoutFactory.Param(Duration.Top());
    MaskCancelFilter.Param maskCancelFilterParam = new MaskCancelFilter.Param(false);
    RequestSemaphoreFilter.Param requestSemaphoreFilterParam =
      new RequestSemaphoreFilter.Param(Integer.MAX_VALUE);
    LoadBalancerFactory.HostStats loadBalancerFactoryHostStats =
      new LoadBalancerFactory.HostStats(new NullStatsReceiver());
    LoadBalancerFactory.Param loadBalancerFactoryParam =
      new LoadBalancerFactory.Param(com.twitter.finagle.loadbalancer.DefaultBalancerFactory.get());
    Netty3Transporter.ChannelFactory netty3TransporterChannelFactory =
      new Netty3Transporter.ChannelFactory(null);
    Netty3Timer netty3Timer = new Netty3Timer(com.twitter.finagle.util.DefaultTimer.get());
    Listener.Backlog listenerBacklog = new Listener.Backlog(Option.empty());
    ExpiringService.Param expiringServiceParam =
      new ExpiringService.Param(Duration.Top(), Duration.Top());
    FailFastFactory.FailFast failFastFactoryFailFast =
      new FailFastFactory.FailFast(true);
    FailureAccrualFactory.Param failureAccrualFactoryParam =
      new FailureAccrualFactory.Param(0, Duration.Top());
    TimeoutFilter.Param timeoutFilterParam = new TimeoutFilter.Param(Duration.Top());
    Transport.BufferSizes transportBufferSizes =
      new Transport.BufferSizes(Option.empty(), Option.empty());
    Transport.Liveness transportLiveness =
      new Transport.Liveness(Duration.Top(), Duration.Top(), Option.empty());
    Transport.Verbose transportVerbose = new Transport.Verbose(false);
    Transport.TLSClientEngine transportTLSClientEngine =
      new Transport.TLSClientEngine(Option.<scala.Function1<SocketAddress, Engine>>empty());
    Transport.TLSServerEngine transportTLSServerEngine =
      new Transport.TLSServerEngine(Option.<scala.Function0<Engine>>empty());
  }
}
