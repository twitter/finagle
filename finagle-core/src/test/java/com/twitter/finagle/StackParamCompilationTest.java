package com.twitter.finagle;

import java.net.SocketAddress;

import scala.Option;
import scala.Some;

import org.junit.Test;

import com.twitter.concurrent.AsyncSemaphore;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.client.DefaultPool;
import com.twitter.finagle.client.StackClient;
import com.twitter.finagle.client.Transporter;
import com.twitter.finagle.factory.BindingFactory;
import com.twitter.finagle.factory.TimeoutFactory;
import com.twitter.finagle.filter.MaskCancelFilter;
import com.twitter.finagle.filter.RequestSemaphoreFilter;
import com.twitter.finagle.loadbalancer.Balancers;
import com.twitter.finagle.loadbalancer.LoadBalancerFactory;
import com.twitter.finagle.netty3.Netty3Transporter;
import com.twitter.finagle.netty3.channel.IdleConnectionFilter;
import com.twitter.finagle.netty3.channel.OpenConnectionsThresholds;
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
import com.twitter.finagle.service.Retries;
import com.twitter.finagle.service.RetryBudgets;
import com.twitter.finagle.service.RetryPolicy;
import com.twitter.finagle.service.TimeoutFilter;
import com.twitter.finagle.service.exp.FailureAccrualPolicies;
import com.twitter.finagle.socks.SocksProxyFlags;
import com.twitter.finagle.ssl.Engine;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.transport.Transport;
import com.twitter.finagle.util.Rngs;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.NullMonitor;
import com.twitter.util.RootMonitor;

public class StackParamCompilationTest {

  @Test
  public void testParams() {
    StackClient<String, String> client =
      ClientBuilder.<String, String>stackClientOfCodec(null)
        .configured(new Label("").mk())
        .configured(new Timer(com.twitter.finagle.util.DefaultTimer.twitter()).mk())
        .configured(new Logger(java.util.logging.Logger.getLogger("com.twitter.finagle")).mk())
        .configured(new Stats(com.twitter.finagle.stats.DefaultStatsReceiver.get()).mk())
        .configured(new Monitor(RootMonitor.getInstance()).mk())
        .configured(new Monitor(NullMonitor.getInstance()).mk())
        .configured(new Reporter(com.twitter.finagle.util.LoadedReporterFactory.get()).mk())
        .configured(new Retries.Budget(RetryBudgets.EMPTY).mk())
        .configured(new Retries.Policy(RetryPolicy.Never()).mk())
        .configured(new Tracer(com.twitter.finagle.tracing.DefaultTracer.get()).mk())
        .configured(new FactoryToService.Enabled(true).mk())
        .configured(new IdleConnectionFilter.Param(Option.<OpenConnectionsThresholds>empty()).mk())
        .configured(
          new DefaultPool.Param(0, Integer.MAX_VALUE, 0, Duration.Top(), Integer.MAX_VALUE).mk())
        .configured(new Transporter.ConnectTimeout(Duration.Top()).mk())
        .configured(new Transporter.TLSHostname(Option.<String>empty()).mk())
        .configured(
          new Transporter.SocksProxy(
            SocksProxyFlags.socksProxy(),
            SocksProxyFlags.socksUsernameAndPassword()).mk())
        .configured(
          new Transporter.HttpProxy(
            Option.<SocketAddress>empty(),
            Option.<Transporter.Credentials>empty()).mk())
        .configured(
          new BindingFactory.BaseDtab(new Function0<Dtab>() {
              public Dtab apply() { return Dtab.empty(); }
          }).mk())
        .configured(new TimeoutFactory.Param(Duration.Top()).mk())
        .configured(new MaskCancelFilter.Param(false).mk())
        .configured(new RequestSemaphoreFilter.Param(
          new Some<AsyncSemaphore>(new AsyncSemaphore(Integer.MAX_VALUE, 0))).mk())
        .configured(new LoadBalancerFactory.HostStats(new NullStatsReceiver()).mk())
        .configured(new LoadBalancerFactory.Param(Balancers.p2c(5, Rngs.threadLocal())).mk())
        .configured(new Netty3Transporter.ChannelFactory(null).mk())
        .configured(new Netty3Timer(com.twitter.finagle.util.DefaultTimer.get().netty()).mk())
        .configured(new Listener.Backlog(Option.empty()).mk())
        .configured(new ExpiringService.Param(Duration.Top(), Duration.Top()).mk())
          .configured(new FailFastFactory.FailFast(true).mk())
        .configured(FailureAccrualFactory.Param(10, Duration.Bottom()).mk())
        .configured(FailureAccrualFactory.Param(FailureAccrualPolicies.newConsecutiveFailuresPolicy(
          3, Duration.fromSeconds(0))).mk())
        .configured(new TimeoutFilter.Param(Duration.Top()).mk())
        .configured(new Transport.BufferSizes(Option.empty(), Option.empty()).mk())
        .configured(new Transport.Liveness(Duration.Top(), Duration.Top(), Option.empty()).mk())
        .configured(new Transport.Verbose(false).mk())
        .configured(new Transporter.TrafficClass(new Some<Object>(1)).mk())
        .configured(new Listener.TrafficClass(Option.empty()).mk())
        .configured(
          new Transport.TLSClientEngine(
            Option.<scala.Function1<SocketAddress, Engine>>empty()
          ).mk())
        .configured(new Transport.TLSServerEngine(Option.<scala.Function0<Engine>>empty()).mk());

    StackClient<String, String> client1 =
        new ClientBuilder()
        .failFast(true)
        .<String, String>stackClientOfCodec(null);
  }

  @Test
  public void testModule1() {
    // or use FactoryToService.Enabled$.MODULE$.param
    Stack.Param<FactoryToService.Enabled> param =
      new FactoryToService.Enabled(true).mk()._2();

    Stackable<Integer> module1 =
      new Stack.Module1<FactoryToService.Enabled, Integer>(param) {
        @Override
        public Stack.Role role() {
          return new Stack.Role("role");
        }

        @Override
        public String description() {
          return "description";
        }

        @Override
        public Integer make(FactoryToService.Enabled p1, Integer next) {
          return next;
        }
      };
  }
}
