package com.twitter.finagle

import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats._
import com.twitter.finagle.thrift._
import com.twitter.finagle.util.Showable
import com.twitter.util.NonFatal
import java.lang.reflect.{Constructor, Method}
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import scala.language.existentials
import scala.reflect.ClassTag

private[twitter] object ThriftUtil {
  private type BinaryService = Service[Array[Byte], Array[Byte]]

  private val thriftFinagleClientParamTypes =
    Seq(classOf[Service[_, _]], classOf[TProtocolFactory])

  private val thriftFinagleClientWithRepClassifierParamTypes =
    Seq(classOf[Service[_, _]], classOf[TProtocolFactory], classOf[ResponseClassifier])

  private val scrooge2FinagleClientParamTypes =
    Seq(
      classOf[Service[_, _]],
      classOf[TProtocolFactory],
      classOf[Option[_]],
      classOf[StatsReceiver])

  private val scrooge3FinagleClientParamTypes =
    Seq(
      classOf[Service[_, _]],
      classOf[TProtocolFactory],
      classOf[String],
      classOf[StatsReceiver])

  private val scrooge3FinagleClientWithRepClassifierParamTypes =
    Seq(
      classOf[Service[_, _]],
      classOf[TProtocolFactory],
      classOf[String],
      classOf[StatsReceiver],
      classOf[ResponseClassifier])

  def findClass1(name: String): Option[Class[_]] =
    try Some(Class.forName(name)) catch {
      case _: ClassNotFoundException => None
    }

  def findClass[A](name: String): Option[Class[A]] =
    for {
      cls <- findClass1(name)
    } yield cls.asInstanceOf[Class[A]]

  def findConstructor[A](clz: Class[A], paramTypes: Class[_]*): Option[Constructor[A]] =
    try {
      Some(clz.getConstructor(paramTypes: _*))
    } catch {
      case _: NoSuchMethodException => None
    }

  def findMethod(clz: Class[_], name: String, params: Class[_]*): Option[Method] =
    try Some(clz.getMethod(name, params:_*)) catch {
      case _: NoSuchMethodException => None
    }

  def findRootWithSuffix(str: String, suffix: String): Option[String] =
    if (str.endsWith(suffix))
      Some(str.stripSuffix(suffix))
    else
      None

  lazy val findSwiftClass: Class[_] => Option[Class[_]] = {
    val f = for {
      serviceSym <- findClass1("com.twitter.finagle.exp.swift.ServiceSym")
      meth <- findMethod(serviceSym, "isService", classOf[Class[_]])
    } yield {
      k: Class[_] =>
        try {
          if (meth.invoke(null, k).asInstanceOf[Boolean]) Some(k)
          else None
        } catch {
          case NonFatal(_) => None
        }
    }

    f getOrElse Function.const(None)
  }

  /**
   * Construct an `Iface` based on an underlying [[com.twitter.finagle.Service]]
   * using whichever Thrift code-generation toolchain is available.
   */
  def constructIface[Iface](
    underlying: Service[ThriftClientRequest, Array[Byte]],
    cls: Class[_],
    protocolFactory: TProtocolFactory,
    sr: StatsReceiver,
    responseClassifier: ResponseClassifier
  ): Iface = {
    val clsName = cls.getName

    def tryThriftFinagleClientRepClassifier: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$ServiceIface")
        clientCls  <- findClass[Iface](baseName + "$ServiceToClient")
        cons       <- findConstructor(clientCls, thriftFinagleClientWithRepClassifierParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, responseClassifier)

    def tryThriftFinagleClient: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$ServiceIface")
        clientCls  <- findClass[Iface](baseName + "$ServiceToClient")
        cons       <- findConstructor(clientCls, thriftFinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory)

    def tryScrooge3FinagleClient: Option[Iface] =
      for {
        clientCls  <- findClass[Iface](clsName + "$FinagleClient")
        cons       <- findConstructor(clientCls, scrooge3FinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, "", sr)

    def tryScrooge3FinagledClientRepClassifier: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$FutureIface")
        clientCls  <- findClass[Iface](baseName + "$FinagledClient")
        cons       <- findConstructor(clientCls, scrooge3FinagleClientWithRepClassifierParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, "", sr, responseClassifier)

    def tryScrooge3FinagledClient: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$FutureIface")
        clientCls  <- findClass[Iface](baseName + "$FinagledClient")
        cons       <- findConstructor(clientCls, scrooge3FinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, "", sr)

    def tryScrooge2Client: Option[Iface] =
      for {
        baseName   <- findRootWithSuffix(clsName, "$FutureIface")
        clientCls  <- findClass[Iface](baseName + "$FinagledClient")
        cons       <- findConstructor(clientCls, scrooge2FinagleClientParamTypes: _*)
      } yield cons.newInstance(underlying, protocolFactory, None, sr)

    def trySwiftClient: Option[Iface] =
      for {
        swiftClass <- findSwiftClass(cls)
        proxy <- findClass1("com.twitter.finagle.exp.swift.SwiftProxy")
        meth <- findMethod(proxy, "newClient",
          classOf[Service[_, _]], classOf[ClassTag[_]])
      } yield {
        val manifest = ClassTag(swiftClass).asInstanceOf[ClassTag[Iface]]
        meth.invoke(null, underlying, manifest).asInstanceOf[Iface]
      }

    val iface =
      tryThriftFinagleClientRepClassifier orElse
      tryThriftFinagleClient orElse
      tryScrooge3FinagleClient orElse
      tryScrooge3FinagledClientRepClassifier orElse
      tryScrooge3FinagledClient orElse
      tryScrooge2Client orElse
      trySwiftClient

    iface getOrElse {
      throw new IllegalArgumentException("Iface %s is not a valid thrift iface".format(clsName))
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
    def tryThriftFinagleService(iface: Class[_]): Option[BinaryService] =
      for {
        baseName   <- findRootWithSuffix(iface.getName, "$ServiceIface")
        serviceCls <- findClass[BinaryService](baseName + s"$$Service")
        cons       <- findConstructor(serviceCls, iface, classOf[TProtocolFactory])
      } yield cons.newInstance(impl, protocolFactory)

    def tryScroogeFinagleService(iface: Class[_]): Option[BinaryService] =
      (for {
        baseName   <- findRootWithSuffix(iface.getName, "$FutureIface") orElse
          Some(iface.getName)
        serviceCls <- findClass[BinaryService](baseName + "$FinagleService") orElse
          findClass[BinaryService](baseName + "$FinagledService")
        baseClass  <- findClass1(baseName)
      } yield {
        // The new constructor takes one more 'label' paramater than the old one, so we first try find
        // the new constructor, it it doesn't not exist, fallback to the one without 'label' parameter.
        val oldParameters = Seq(baseClass, classOf[TProtocolFactory], classOf[StatsReceiver], Integer.TYPE)
        val newParameters = oldParameters :+ classOf[String]
        val oldArgs = Seq(impl, protocolFactory, stats, Int.box(maxThriftBufferSize))
        val newArgs = oldArgs :+ label
        def newConsCall: Option[BinaryService] = findConstructor(serviceCls, newParameters: _*).map(
          cons => cons.newInstance(newArgs: _*)
        )
        def oldConsCall: Option[BinaryService] = findConstructor(serviceCls, oldParameters: _*).map(
          cons => cons.newInstance(oldArgs: _*)
        )
        newConsCall.orElse(oldConsCall)
      }).flatten

    // The legacy $FinagleService that doesn't take stats.
    def tryLegacyScroogeFinagleService(iface: Class[_]): Option[BinaryService] =
      for {
        baseName   <- findRootWithSuffix(iface.getName, "$FutureIface") orElse
          Some(iface.getName)
        serviceCls <- findClass[BinaryService](baseName + "$FinagleService") orElse
          findClass[BinaryService](baseName + "$FinagledService")
        cons       <- findConstructor(serviceCls, iface, classOf[TProtocolFactory])
      } yield cons.newInstance(impl, protocolFactory)

    def trySwiftService(iface: Class[_]): Option[BinaryService] =
      for {
        _ <- findSwiftClass(iface)
        swiftServiceCls <- findClass1("com.twitter.finagle.exp.swift.SwiftService")
        const <- findConstructor(swiftServiceCls, classOf[Object])
      } yield const.newInstance(impl).asInstanceOf[BinaryService]

    def tryClass(cls: Class[_]): Option[BinaryService] =
      tryThriftFinagleService(cls) orElse
      tryScroogeFinagleService(cls) orElse
      tryLegacyScroogeFinagleService(cls) orElse
      trySwiftService(cls) orElse
      (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(tryClass).headOption

    tryClass(impl.getClass).getOrElse {
      throw new IllegalArgumentException("argument implements no candidate ifaces")
    }
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   * (Legacy version for backward-compatibility).
   */
  def serverFromIface(impl: AnyRef, protocolFactory: TProtocolFactory, serviceName: String): BinaryService = {
    serverFromIface(impl, protocolFactory, LoadedStatsReceiver, Thrift.Server.maxThriftBufferSize, serviceName)
  }
}

/**
 * A mixin trait to provide a rich Thrift client API.
 *
 * @define clientUse
 *
 * Create a new client of type `Iface`, which must be generated
 * by either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
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
 *   val loggerService = Thrift.client.newServiceIface[Logger.ServiceIface]("localhost:8000")
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
   * Both [[defaultClientName]] and [[stats]] predate Params`
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
  @deprecated("Use destination names via newIface(String) or newIface(Name)", "6.7.x")
  def newIface[Iface: ClassTag](group: Group[SocketAddress]): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](group, cls)
  }

  /**
   * $clientUse
   */
  @deprecated("Use destination names via newIface(String) or newIface(Name)", "6.7.x")
  def newIface[Iface](group: Group[SocketAddress], cls: Class[_]): Iface = group match {
    case LabelledGroup(g, label) => newIface(Name.fromGroup(g), label, cls)
    case _ => newIface(Name.fromGroup(group), "", cls)
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
  def newServiceIface[ServiceIface](dest: String, label: String)(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val thriftService = newService(dest, label)
    val statsLabel = if (label.isEmpty) defaultClientName else label
    val scopedStats = stats.scope(statsLabel)
    builder.newServiceIface(thriftService, protocolFactory, scopedStats)
  }

  /**
   * $serviceIface
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to.
   * @param label Assign a label for scoped stats.
   */
  def newServiceIface[ServiceIface](dest: Name, label: String)(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val thriftService = newService(dest, label)
    val statsLabel = if (label.isEmpty) defaultClientName else label
    val scopedStats = stats.scope(statsLabel)
    builder.newServiceIface(thriftService, protocolFactory, scopedStats)
  }

  /**
   * $serviceIface
   */
  @deprecated("Must provide service label", "2015-10-26")
  def newServiceIface[ServiceIface](dest: String)(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = newServiceIface(dest, "")

  /**
   * $serviceIface
   */
  @deprecated("Must provide service label", "2015-10-26")
  def newServiceIface[ServiceIface](dest: Name)(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = newServiceIface(dest, "")

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
