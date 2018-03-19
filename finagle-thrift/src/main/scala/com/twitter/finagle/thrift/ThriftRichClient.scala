package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.service.{Filterable, ServicePerEndpointBuilder}
import com.twitter.finagle.util.Showable
import com.twitter.finagle.{Client, Name, Resolver, Service, Stack}
import org.apache.thrift.protocol.TProtocolFactory
import scala.reflect.ClassTag

/**
 * A mixin trait to provide a rich Thrift client API.
 *
 * @define clientUse
 *
 * Create a new client of type `Iface`, which must be generated
 * by [[https://github.com/twitter/scrooge Scrooge]].
 *
 * For Scala generated code, the `Class` passed in should be
 * either `ServiceName$FutureIface` or `ServiceName[Future]`.
 *
 * For Java generated code, the `Class` passed in should be
 * `ServiceName$ServiceIface`.
 *
 *
 * @define build
 *
 * Create a new client of type `ThriftService`, which must be generated
 * by [[https://github.com/twitter/scrooge Scrooge]].
 *
 * For Scala generated code, the `Class` passed in should be
 * either `ServiceName$MethodPerEndpoint` or `ServiceName[Future]`.
 *
 * For Java generated code, the `Class` passed in should be
 * `ServiceName$ServiceIface`.
 *
 * @define serviceIface
 *
 * Construct a Finagle `Service` interface for a Scrooge-generated Thrift object.
 *
 * E.g. given a Thrift service
 * {{{
 *   service Logger {
 *     string log(1: string message, 2: i32 logLevel);
 *     i32 getLogSize();
 *   }
 * }}}
 *
 * you can construct a client interface with a Finagle Service per Thrift method:
 *
 * {{{
 *   val loggerService: Logger.ServiceIface = Thrift.client.newServiceIface[Logger.ServiceIface]("localhost:8000", "client_label")
 *   val response: String = loggerService.log(Logger.Log.Args("log message", 1))
 * }}}
 *
 * @define servicePerEndpoint
 *
 * Construct a Finagle `Service` interface for a Scrooge-generated Thrift object.
 *
 * E.g. given a Thrift service
 * {{{
 *   service Logger {
 *     string log(1: string message, 2: i32 logLevel);
 *     i32 getLogSize();
 *   }
 * }}}
 *
 * you can construct a client interface with a Finagle Service per Thrift method:
 *
 * {{{
 *   val loggerService: Logger.ServicePerEndpoint = Thrift.client.servicePerEndpoint[Logger.ServicePerEndpoint]("localhost:8000", "client_label")
 *   val response: String = loggerService.log(Logger.Log.Args("log message", 1))
 * }}}
 *
 *
 * You can also create a Finagle `Service[scrooge.Request, scrooge.Response]` per-endpoint (method)
 * for a Scrooge-generated Thrift object.
 *
 * E.g. given a Thrift service
 * {{{
 *   service Logger {
 *     string log(1: string message, 2: i32 logLevel);
 *     i32 getLogSize();
 *   }
 * }}}
 *
 * you can construct a client interface with a Finagle Service per Thrift method that uses the Scrooge
 * Request/Response envelopes:
 *
 * {{{
 *   val loggerService: Logger.ReqRepServicePerEndpoint = Thrift.client.servicePerEndpoint[Logger.ReqRepServicePerEndpoint]("localhost:8000", "client_label")
 *   val response: c.t.scrooge.Response[String] = loggerService.log(c.t.scrooge.Request(Logger.Log.Args("log message", 1)))
 * }}}
 *
 * This allows you to access any contained `c.t.scrooge.Request` and `c.t.scrooge.Response` headers.
 *
 * @define buildMultiplexClient
 *
 * Build client interfaces for multiplexed thrift services.
 *
 * E.g.
 * {{{
 *   val client = Thrift.client.multiplex(address, "client") { client =>
 *     new {
 *       val echo = client.newIface[Echo.FutureIface]("echo")
 *       val extendedEcho = client.newServiceIface[ExtendedEcho.ServiceIface]("extendedEcho")
 *     }
 *   }
 *
 *   client.echo.echo("hello")
 *   client.extendedEcho.getStatus(ExtendedEcho.GetStatus.Args())
 * }}}
 */
trait ThriftRichClient { self: Client[ThriftClientRequest, Array[Byte]] =>
  import ThriftUtil._

  protected val clientParam: RichClientParam

  protected def protocolFactory: TProtocolFactory

  /** The client name used when group isn't named. */
  protected val defaultClientName: String
  protected def stats: StatsReceiver = ClientStatsReceiver

  /**
   * The `Stack.Params` to be used by this client.
   *
   * Both [[defaultClientName]] and [[stats]] predate [[Stack.Params]]
   * and as such are implemented separately.
   */
  protected def params: Stack.Params

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface](dest: String, cls: Class[_]): Iface = {
    val (n, l) = Resolver.evalLabeled(dest)
    newIface(n, l, cls)
  }

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface](dest: String, label: String, cls: Class[_]): Iface =
    newIface(Resolver.eval(dest), label, cls)

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface: ClassTag](dest: String): Iface = {
    val (n, l) = Resolver.evalLabeled(dest)
    newIface[Iface](n, l)
  }

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface: ClassTag](dest: String, label: String): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](Resolver.eval(dest), label, cls)
  }

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface: ClassTag](dest: Name, label: String): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](dest, label, cls)
  }

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface](name: Name, label: String, cls: Class[_]): Iface = {
    newIface(name, label, cls, clientParam, newService(name, label))
  }

  /**
   * $clientUse
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#build", "2017-11-20")
  def newIface[Iface](
    name: Name,
    label: String,
    cls: Class[_],
    clientParam: RichClientParam,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Iface = {
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, l1) => l0
    }

    val clientConfigScoped =
      clientParam.copy(clientStats = clientParam.clientStats.scope(clientLabel))
    constructIface(service, cls, clientConfigScoped)
  }

  /**
   * $build
   */
  def build[ThriftService](dest: String, cls: Class[_]): ThriftService = {
    val (n, l) = Resolver.evalLabeled(dest)
    build(n, l, cls)
  }

  /**
   * $build
   */
  def build[ThriftService](dest: String, label: String, cls: Class[_]): ThriftService =
    build(Resolver.eval(dest), label, cls)

  /**
   * $build
   */
  def build[ThriftService: ClassTag](dest: String): ThriftService = {
    val (n, l) = Resolver.evalLabeled(dest)
    build[ThriftService](n, l)
  }

  /**
   * $build
   */
  def build[ThriftService: ClassTag](dest: String, label: String): ThriftService = {
    val cls = implicitly[ClassTag[ThriftService]].runtimeClass
    build[ThriftService](Resolver.eval(dest), label, cls)
  }

  /**
   * $build
   */
  def build[ThriftService: ClassTag](dest: Name, label: String): ThriftService = {
    val cls = implicitly[ClassTag[ThriftService]].runtimeClass
    build[ThriftService](dest, label, cls)
  }

  /**
   * $build
   */
  def build[ThriftService](name: Name, label: String, cls: Class[_]): ThriftService = {
    build(name, label, cls, clientParam, newService(name, label))
  }

  /**
   * $build
   */
  def build[ThriftService](
    name: Name,
    label: String,
    cls: Class[_],
    clientParam: RichClientParam,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): ThriftService = {
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, l1) => l0
    }

    val clientConfigScoped =
      clientParam.copy(clientStats = clientParam.clientStats.scope(clientLabel))
    constructIface(service, cls, clientConfigScoped)
  }

  @deprecated("Use com.twitter.finagle.thrift.RichClientParam", "2017-8-16")
  def newIface[Iface](
    name: Name,
    label: String,
    cls: Class[_],
    protocolFactory: TProtocolFactory,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Iface = {
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, l1) => l0
    }

    val clientConfigScoped = clientParam.copy(
      protocolFactory = protocolFactory,
      clientStats = clientParam.clientStats.scope(clientLabel)
    )
    constructIface(service, cls, clientConfigScoped)
  }

  /**
   * $serviceIface
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to, in the format accepted by `Resolver.eval`.
   * @param label Assign a label for scoped stats.
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#servicePerEndpoint[ServicePerEndpoint]", "2017-11-13")
  def newServiceIface[ServiceIface <: Filterable[ServiceIface]](
    dest: String,
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface =
    newServiceIface(Resolver.eval(dest), label)

  /**
   * $serviceIface
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to.
   * @param label Assign a label for scoped stats.
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#servicePerEndpoint[ServicePerEndpoint]", "2017-11-13")
  def newServiceIface[ServiceIface <: Filterable[ServiceIface]](
    dest: Name,
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val thriftService = newService(dest, label)
    newServiceIface(thriftService, label)
  }

  /**
   * $serviceIface
   *
   * @param service The Finagle [[Service]] to be used.
   * @param label Assign a label for scoped stats.
   * @param builder The builder type is generated by Scrooge for a thrift service.
   */
  @deprecated("Use com.twitter.finagle.ThriftRichClient#servicePerEndpoint[ServicePerEndpoint]", "2017-11-13")
  private[finagle] def newServiceIface[ServiceIface <: Filterable[ServiceIface]](
    service: Service[ThriftClientRequest, Array[Byte]],
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val statsLabel = if (label.isEmpty) defaultClientName else label
    val clientConfigScoped =
      clientParam.copy(clientStats = clientParam.clientStats.scope(statsLabel))
    builder.newServiceIface(service, clientConfigScoped)
  }

  /**
   * $servicePerEndpoint
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to, in the format accepted by `Resolver.eval`.
   * @param label Assign a label for scoped stats.
   */
  def servicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    dest: String,
    label: String
  )(
    implicit builder: ServicePerEndpointBuilder[ServicePerEndpoint]
  ): ServicePerEndpoint =
    servicePerEndpoint(Resolver.eval(dest), label)

  /**
   * $servicePerEndpoint
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to.
   * @param label Assign a label for scoped stats.
   */
  def servicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    dest: Name,
    label: String
  )(
    implicit builder: ServicePerEndpointBuilder[ServicePerEndpoint]
  ): ServicePerEndpoint = {
    val thriftService = newService(dest, label)
    servicePerEndpoint(thriftService, label)
  }

  /**
   * $servicePerEndpoint
   *
   * @param service The Finagle [[Service]] to be used.
   * @param label Assign a label for scoped stats.
   * @param builder The builder type is generated by Scrooge for a thrift service.
   */
  private[finagle] def servicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
    service: Service[ThriftClientRequest, Array[Byte]],
    label: String
  )(
    implicit builder: ServicePerEndpointBuilder[ServicePerEndpoint]
  ): ServicePerEndpoint = {
    val statsLabel = if (label.isEmpty) defaultClientName else label
    val clientConfigScoped =
      clientParam.copy(clientStats = clientParam.clientStats.scope(statsLabel))
    builder.servicePerEndpoint(service, clientConfigScoped)
  }

  /**
   * $buildMultiplexClient
   */
  def multiplex[T](dest: Name, label: String)(build: MultiplexedThriftClient => T): T = {
    build(new MultiplexedThriftClient(dest, label))
  }

  /**
   * $buildMultiplexClient
   */
  def multiplex[T](dest: String, label: String)(build: MultiplexedThriftClient => T): T = {
    multiplex(Resolver.eval(dest), label)(build)
  }

  class MultiplexedThriftClient(dest: Name, label: String) {

    private[this] val service = newService(dest, label)

    @deprecated("Use com.twitter.finagle.ThriftRichClient.MultiplexedThriftClient.build", "2017-11-20")
    def newIface[Iface: ClassTag](serviceName: String): Iface = {
      val cls = implicitly[ClassTag[Iface]].runtimeClass
      newIface[Iface](serviceName, cls)
    }

    @deprecated("Use com.twitter.finagle.ThriftRichClient.MultiplexedThriftClient.build", "2017-11-20")
    def newIface[Iface](serviceName: String, cls: Class[_]): Iface = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val clientConfigMultiplexed = clientParam.copy(protocolFactory = multiplexedProtocol)
      ThriftRichClient.this.newIface(dest, label, cls, clientConfigMultiplexed, service)
    }

    def build[ThriftService: ClassTag](serviceName: String): ThriftService = {
      val cls = implicitly[ClassTag[ThriftService]].runtimeClass
      build[ThriftService](serviceName, cls)
    }

    def build[ThriftService](serviceName: String, cls: Class[_]): ThriftService = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val clientConfigMultiplexed = clientParam.copy(protocolFactory = multiplexedProtocol)
      ThriftRichClient.this.build(dest, label, cls, clientConfigMultiplexed, service)
    }

    @deprecated("Use com.twitter.finagle.ThriftRichClient.MultiplexedThriftClient#servicePerEndpoint", "2017-11-13")
    def newServiceIface[ServiceIface <: Filterable[ServiceIface]](
      serviceName: String
    )(
      implicit builder: ServiceIfaceBuilder[ServiceIface]
    ): ServiceIface = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val statsLabel = if (label.isEmpty) defaultClientName else label
      val clientConfigMultiplexedScoped = clientParam.copy(
        protocolFactory = multiplexedProtocol,
        clientStats = clientParam.clientStats.scope(statsLabel)
      )
      builder.newServiceIface(service, clientConfigMultiplexedScoped)
    }

    def servicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
      serviceName: String
    )(
      implicit builder: ServicePerEndpointBuilder[ServicePerEndpoint]
    ): ServicePerEndpoint = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val statsLabel = if (label.isEmpty) defaultClientName else label
      val clientConfigMultiplexedScoped = clientParam.copy(
        protocolFactory = multiplexedProtocol,
        clientStats = clientParam.clientStats.scope(statsLabel)
      )
      builder.servicePerEndpoint(service, clientConfigMultiplexedScoped)
    }
  }
}
