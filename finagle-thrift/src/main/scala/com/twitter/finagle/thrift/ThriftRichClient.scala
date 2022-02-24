package com.twitter.finagle.thrift

import com.twitter.finagle.client.StackBasedClient
import com.twitter.finagle.stats.ClientStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift.service.Filterable
import com.twitter.finagle.thrift.service.ServicePerEndpointBuilder
import com.twitter.finagle.util.Showable
import com.twitter.finagle.Name
import com.twitter.finagle.Resolver
import com.twitter.finagle.Service
import com.twitter.finagle.Thrift
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
 * either `ServiceName$MethodPerEndpoint` or `ServiceName[Future]`.
 *
 * For Java generated code, the `Class` passed in should be
 * `ServiceName$ServiceIface`.
 *
 *
 * @define build
 *
 * Create a new client of type `ThriftServiceType`, which must be generated
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
 *       val echo = client.newIface[Echo.MethodPerEndpoint]("echo")
 *       val extendedEcho = client.newServiceIface[ExtendedEcho.ServiceIface]("extendedEcho")
 *     }
 *   }
 *
 *   client.echo.echo("hello")
 *   client.extendedEcho.getStatus(ExtendedEcho.GetStatus.Args())
 * }}}
 */
trait ThriftRichClient { self: StackBasedClient[ThriftClientRequest, Array[Byte]] =>
  import ThriftUtil._

  protected val clientParam: RichClientParam

  /** The client name used when group isn't named. */
  protected val defaultClientName: String
  protected def stats: StatsReceiver = ClientStatsReceiver

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
    val service =
      self
        .configured(Thrift.param.ServiceClass(Some(cls)))
        .newService(name, label)

    newIface(name, label, cls, clientParam, service)
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
  // NB: ThriftServiceType is used to avoid a naming collision with c.t.f.thrift.GeneratedThriftService
  def build[ThriftServiceType](dest: String, cls: Class[_]): ThriftServiceType = {
    val (n, l) = Resolver.evalLabeled(dest)
    build(n, l, cls)
  }

  /**
   * $build
   */
  def build[ThriftServiceType](dest: String, label: String, cls: Class[_]): ThriftServiceType =
    build(Resolver.eval(dest), label, cls)

  /**
   * $build
   */
  def build[ThriftServiceType: ClassTag](dest: String): ThriftServiceType = {
    val (n, l) = Resolver.evalLabeled(dest)
    build[ThriftServiceType](n, l)
  }

  /**
   * $build
   */
  def build[ThriftServiceType: ClassTag](dest: String, label: String): ThriftServiceType = {
    val cls = implicitly[ClassTag[ThriftServiceType]].runtimeClass
    build[ThriftServiceType](Resolver.eval(dest), label, cls)
  }

  /**
   * $build
   */
  def build[ThriftServiceType: ClassTag](dest: Name, label: String): ThriftServiceType = {
    val cls = implicitly[ClassTag[ThriftServiceType]].runtimeClass
    build[ThriftServiceType](dest, label, cls)
  }

  /**
   * $build
   */
  def build[ThriftServiceType](name: Name, label: String, cls: Class[_]): ThriftServiceType = {
    val service =
      self
        .configured(Thrift.param.ServiceClass(Some(cls)))
        .newService(name, label)

    build(name, label, cls, clientParam, service)
  }

  /**
   * $build
   */
  def build[ThriftServiceType](
    name: Name,
    label: String,
    cls: Class[_],
    clientParam: RichClientParam,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): ThriftServiceType = {
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, _) => l0
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
  @deprecated(
    "Use com.twitter.finagle.ThriftRichClient#servicePerEndpoint[ServicePerEndpoint]",
    "2017-11-13"
  )
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
  @deprecated(
    "Use com.twitter.finagle.ThriftRichClient#servicePerEndpoint[ServicePerEndpoint]",
    "2017-11-13"
  )
  def newServiceIface[ServiceIface <: Filterable[ServiceIface]](
    dest: Name,
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val service =
      self
        .configured(Thrift.param.ServiceClass(Option(builder.serviceClass)))
        .newService(dest, label)

    newServiceIface(service, label)
  }

  /**
   * $serviceIface
   *
   * @param service The Finagle [[Service]] to be used.
   * @param label Assign a label for scoped stats.
   * @param builder The builder type is generated by Scrooge for a thrift service.
   */
  @deprecated(
    "Use com.twitter.finagle.ThriftRichClient#servicePerEndpoint[ServicePerEndpoint]",
    "2017-11-13"
  )
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
    val service =
      self
        .configured(Thrift.param.ServiceClass(Option(builder.serviceClass)))
        .newService(dest, label)

    newServicePerEndpoint(service, label)
  }

  /**
   * $servicePerEndpoint
   *
   * @param service The Finagle [[Service]] to be used.
   * @param label Assign a label for scoped stats.
   * @param builder The builder type is generated by Scrooge for a thrift service.
   */
  protected def newServicePerEndpoint[ServicePerEndpoint <: Filterable[ServicePerEndpoint]](
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

    // NB: ThriftServiceType is used to avoid a naming collision with c.t.f.thrift.GeneratedThriftService
    def build[ThriftServiceType: ClassTag](serviceName: String): ThriftServiceType = {
      val cls = implicitly[ClassTag[ThriftServiceType]].runtimeClass
      build[ThriftServiceType](serviceName, cls)
    }

    def build[ThriftServiceType](serviceName: String, cls: Class[_]): ThriftServiceType = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val clientConfigMultiplexed = clientParam.copy(protocolFactory = multiplexedProtocol)
      ThriftRichClient.this.build(dest, label, cls, clientConfigMultiplexed, service)
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
