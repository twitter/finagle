package com.twitter.finagle

import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats._
import com.twitter.finagle.thrift._
import com.twitter.finagle.util.Showable
import java.lang.reflect.Constructor
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import scala.reflect.ClassTag

private[twitter] object ThriftUtil {
  private type BinaryService = Service[Array[Byte], Array[Byte]]

  private def findClass1(name: String): Option[Class[_]] =
    try Some(Class.forName(name)) catch {
      case _: ClassNotFoundException => None
    }

  private def findClass[A](name: String): Option[Class[A]] =
    for {
      cls <- findClass1(name)
    } yield cls.asInstanceOf[Class[A]]

  private def findConstructor[A](clz: Class[A], paramTypes: Class[_]*): Option[Constructor[A]] =
    try {
      Some(clz.getConstructor(paramTypes: _*))
    } catch {
      case _: NoSuchMethodException => None
    }

  private def findRootWithSuffix(str: String, suffix: String): Option[String] =
    if (str.endsWith(suffix))
      Some(str.stripSuffix(suffix))
    else
      None

  /**
   * Construct an `Iface` based on an underlying [[com.twitter.finagle.Service]]
   * using whichever Thrift code-generation toolchain is available.
   */
  private[finagle] def constructIface[Iface](
    underlying: Service[ThriftClientRequest, Array[Byte]],
    cls: Class[_],
    protocolFactory: TProtocolFactory,
    sr: StatsReceiver,
    responseClassifier: ResponseClassifier
  ): Iface = {
    val clsName = cls.getName

    // This is used with Scrooge's Java generated code.
    // The class name passed in should be ServiceName$ServiceIface.
    // Will try to create a ServiceName$ServiceToClient instance.
    def tryJavaServiceNameDotServiceIface: Option[Iface] =
      for {
        baseName <- findRootWithSuffix(clsName, "$ServiceIface")
        clientCls <- findClass[Iface](baseName + "$ServiceToClient")
        cons <- findConstructor(clientCls,
          classOf[Service[_, _]], classOf[TProtocolFactory], classOf[ResponseClassifier])
      } yield cons.newInstance(underlying, protocolFactory, responseClassifier)

    // This is used with Scrooge's Scala generated code.
    // The class name passed in should be ServiceName$FutureIface
    // or the higher-kinded version, ServiceName[Future].
    // Will try to create a ServiceName$FinagledClient instance.
    def tryScalaServiceNameIface: Option[Iface] =
      for {
        baseName <- findRootWithSuffix(clsName, "$FutureIface")
          .orElse(Some(clsName))
        clientCls <- findClass[Iface](baseName + "$FinagledClient")
        cons <- findConstructor(clientCls,
          classOf[Service[_, _]], classOf[TProtocolFactory], classOf[String], classOf[StatsReceiver], classOf[ResponseClassifier])
      } yield cons.newInstance(underlying, protocolFactory, "", sr, responseClassifier)

    val iface =
      tryJavaServiceNameDotServiceIface
        .orElse(tryScalaServiceNameIface)

    iface.getOrElse {
      throw new IllegalArgumentException(
        s"Iface $clsName is not a valid thrift iface. For Scala generated code, " +
          "try `YourServiceName$FutureIface` or `YourServiceName[Future]. " +
          "For Java generated code, try `YourServiceName$ServiceIface`.")
    }
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   */
  def serverFromIface(
    impl: AnyRef,
    protocolFactory: TProtocolFactory,
    stats: StatsReceiver,
    maxThriftBufferSize: Int,
    label: String
  ): BinaryService = {
    // This is used with Scrooge's Java generated code.
    // The class passed in should be ServiceName$ServiceIface.
    // Will try to create a ServiceName$Service instance.
    def tryThriftFinagleService(iface: Class[_]): Option[BinaryService] =
      for {
        baseName <- findRootWithSuffix(iface.getName, "$ServiceIface")
        serviceCls <- findClass[BinaryService](baseName + s"$$Service")
        cons <- findConstructor(serviceCls, iface, classOf[TProtocolFactory])
      } yield cons.newInstance(impl, protocolFactory)

    // This is used with Scrooge's Scala generated code.
    // The class passed in should be ServiceName$FutureIface,
    // ServiceName$FutureIface, or ServiceName.
    // Will try to create a ServiceName$FinagleService.
    def tryScroogeFinagleService(iface: Class[_]): Option[BinaryService] =
      (for {
        baseName <- findRootWithSuffix(iface.getName, "$FutureIface")
          // handles ServiceB extends ServiceA, then using ServiceB$MethodIface
          .orElse(findRootWithSuffix(iface.getName, "$MethodIface"))
          .orElse(Some(iface.getName))
        serviceCls <- findClass[BinaryService](baseName + "$FinagleService")
        baseClass <- findClass1(baseName)
      } yield {
        findConstructor(serviceCls,
          baseClass, classOf[TProtocolFactory], classOf[StatsReceiver], Integer.TYPE, classOf[String]
        ).map { cons =>
          cons.newInstance(impl, protocolFactory, stats, Int.box(maxThriftBufferSize), label)
        }
      }).flatten

    def tryClass(cls: Class[_]): Option[BinaryService] =
      tryThriftFinagleService(cls)
        .orElse(tryScroogeFinagleService(cls))
        .orElse {
          (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(tryClass).headOption
        }

    tryClass(impl.getClass).getOrElse {
      throw new IllegalArgumentException(
        s"$impl implements no candidate ifaces. For Scala generated code, " +
          "try `YourServiceName$FutureIface`, `YourServiceName$MethodIface` or `YourServiceName`. " +
          "For Java generated code, try `YourServiceName$ServiceIface`.")
    }
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   * (Legacy version for backward-compatibility).
   */
  def serverFromIface(
    impl: AnyRef,
    protocolFactory: TProtocolFactory,
    serviceName: String
  ): BinaryService =
    serverFromIface(
      impl,
      protocolFactory,
      LoadedStatsReceiver,
      Thrift.Server.maxThriftBufferSize,
      serviceName)
}

/**
 * A mixin trait to provide a rich Thrift client API.
 *
 * @define clientUse
 *
 * Create a new client of type `Iface`, which must be generated
 * [[https://github.com/twitter/scrooge Scrooge]].
 *
 * For Scala generated code, the `Class` passed in should be
 * either `ServiceName$FutureIface` or `ServiceName[Future]`.
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
 *   val loggerService = Thrift.client.newServiceIface[Logger.ServiceIface]("localhost:8000", "client_label")
 *   val response = loggerService.log(Logger.Log.Args("log message", 1))
 * }}}
 */
trait ThriftRichClient { self: Client[ThriftClientRequest, Array[Byte]] =>
  import ThriftUtil._

  protected val protocolFactory: TProtocolFactory
  /** The client name used when group isn't named. */
  protected val defaultClientName: String
  protected lazy val stats: StatsReceiver = ClientStatsReceiver

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
  def newIface[Iface](dest: String, cls: Class[_]): Iface = {
    val (n, l) = Resolver.evalLabeled(dest)
    newIface(n, l, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface](dest: String, label: String, cls: Class[_]): Iface =
    newIface(Resolver.eval(dest), label, cls)

  /**
   * $clientUse
   */
  def newIface[Iface: ClassTag](dest: String): Iface = {
    val (n, l) = Resolver.evalLabeled(dest)
    newIface[Iface](n, l)
  }

  /**
   * $clientUse
   */
  def newIface[Iface: ClassTag](dest: String, label: String): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](Resolver.eval(dest), label, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface: ClassTag](dest: Name, label: String): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](dest, label, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface](name: Name, label: String, cls: Class[_]): Iface = {
    val underlying = newService(name, label)
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, l1) => l0
    }
    val sr = stats.scope(clientLabel)
    val responseClassifier =
      params[com.twitter.finagle.param.ResponseClassifier].responseClassifier

    constructIface(underlying, cls, protocolFactory, sr, responseClassifier)
  }

  /**
   * $serviceIface
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to, in the format accepted by `Resolver.eval`.
   * @param label Assign a label for scoped stats.
   */
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
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
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
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
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
    service: Service[ThriftClientRequest, Array[Byte]],
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val statsLabel = if (label.isEmpty) defaultClientName else label
    val scopedStats = stats.scope(statsLabel)
    builder.newServiceIface(service, protocolFactory, scopedStats)
  }

  /**
   * Converts from a Service interface (`ServiceIface`) to the
   * method interface (`newIface`).
   */
  def newMethodIface[ServiceIface, FutureIface](serviceIface: ServiceIface)(
    implicit builder: MethodIfaceBuilder[ServiceIface, FutureIface]
  ): FutureIface = builder.newMethodIface(serviceIface)
}

/**
 * A mixin trait to provide a rich Thrift server API.
 *
 * @define serveIface
 *
 * Serve the interface implementation `iface`, which must be generated
 * by either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * Given the IDL:
 *
 * {{{
 * service TestService {
 *   string query(1: string x)
 * }
 * }}}
 *
 * Scrooge will generate an interface, `TestService.FutureIface`,
 * implementing the above IDL.
 */
trait ThriftRichServer { self: Server[Array[Byte], Array[Byte]] =>
  import ThriftUtil._

  protected val protocolFactory: TProtocolFactory

  protected val maxThriftBufferSize: Int = Thrift.Server.maxThriftBufferSize

  protected val serverLabel: String = "thrift"

  protected def params: Stack.Params

  protected lazy val serverStats: StatsReceiver = params[Stats].statsReceiver

  /**
   * $serveIface
   */
  def serveIface(addr: String, iface: AnyRef): ListeningServer =
    serve(addr, serverFromIface(iface, protocolFactory, serverStats, maxThriftBufferSize, serverLabel))

  /**
   * $serveIface
   */
  def serveIface(addr: SocketAddress, iface: AnyRef): ListeningServer =
    serve(addr, serverFromIface(iface, protocolFactory, serverStats, maxThriftBufferSize, serverLabel))
}
