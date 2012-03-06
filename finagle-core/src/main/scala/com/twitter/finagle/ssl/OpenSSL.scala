package com.twitter.finagle.ssl

import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{Level, Logger}
import javax.net.ssl._

import collection.mutable.{Map => MutableMap}

/*
 * Creates APR/OpenSSL SSLEngines on behalf of the Ssl singleton
 *
 * You need to have the appropriate shared libraries on your java.library.path
 */
object OpenSSL {
  type MapOfStrings = java.util.Map[java.lang.String, java.lang.String]

  private[this] val log = Logger.getLogger(getClass.getName)


  // For flagging global initialization of APR and OpenSSL
  private[this] val initializedLibrary = new AtomicBoolean(false)

  private[this] var mallocPool: AnyRef = null
  private[this] var bufferPool: AnyRef = null
  private[this] val defaultCiphers =
    "AES128-SHA:RC4:AES:!ADH:!aNULL:!DH:!EDH:!PSK:!ECDH:!eNULL:!LOW:!SSLv2:!EXP:!NULL"

  /*
   * Deal with initialization of the native library
   */
  class Linker {
    private[this] def classNamed(name: String): Class[_] =
     Class.forName("org.apache.tomcat.jni." + name)

    val aprClass = classNamed("Library")
    val aprInitMethod = aprClass.getMethod("initialize", classOf[String])

    val poolClass = classNamed("Pool")
    val poolCreateMethod = poolClass.getMethod("create", classOf[Long])

    val sslClass = classNamed("SSL")
    val sslInitMethod = sslClass.getMethod("initialize", classOf[String])


    // OpenSSLEngine-specific configuration classes
    val bufferPoolClass    = classNamed("ssl.DirectBufferPool")
    val bufferPoolCtor     = bufferPoolClass.getConstructor(classOf[Int])

    val configurationClass = classNamed("ssl.SSLConfiguration")
    val configurationCtor  = configurationClass.getConstructor(classOf[MapOfStrings])

    val contextHolderClass = classNamed("ssl.SSLContextHolder")
    val contextHolderCtor  = contextHolderClass.getConstructor(classOf[Long], configurationClass)

    val sslEngineClass     = classNamed("ssl.OpenSSLEngine")
    val sslEngineCtor      = sslEngineClass.getConstructor(contextHolderClass, bufferPoolClass)

    if (initializedLibrary.compareAndSet(false, true)) {
      aprInitMethod.invoke(aprClass, null)
      sslInitMethod.invoke(sslClass, null)
      mallocPool = poolCreateMethod.invoke(poolClass, 0L.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]

      // We need to know how many workers might need buffers simultaneously, and to allocate a large
      // enough pool.
      val capacity = Runtime.getRuntime().availableProcessors() * 2
      bufferPool = bufferPoolCtor.newInstance(capacity.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    }
  }

  private[this] val contextHolderCache: MutableMap[String, Object] = MutableMap.empty
  private[this] var linker: Linker = null

  /**
   * Get a server
   */
  def server(certificatePath: String,
             keyPath: String,
             caPath: String,
             ciphers: String,
             nextProtos: String,
             useCache: Boolean = true): Option[Engine] = {
    try {
      synchronized {
        if (null == linker) linker = new Linker()
      }
    } catch {
      case e: Exception =>
        // This is a warning rather than a Throwable because we fall back to JSSE
        log.log(Level.FINEST,
                "APR/OpenSSL could not be loaded: " +
                e.getClass().getName() + ": " + e.getMessage())
        return None
    }

    def makeContextHolder = {
      val configMap = new java.util.HashMap[java.lang.String, java.lang.String]
      configMap.put("ssl.cert_path", certificatePath)
      configMap.put("ssl.key_path", keyPath)
      configMap.put("ssl.cipher_spec", Option(ciphers).getOrElse { defaultCiphers })

      if (caPath != null)
        configMap.put("ssl.ca_path", caPath)

      if (nextProtos != null)
        configMap.put("ssl.next_protos", nextProtos)

      val config = linker.configurationCtor.newInstance(configMap.asInstanceOf[MapOfStrings])

      log.finest("OpenSSL context instantiated for certificate '%s'".format(certificatePath))

      linker.contextHolderCtor.newInstance(mallocPool, config.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    }

    val contextHolder = synchronized {
      if (useCache)
        contextHolderCache.getOrElseUpdate(certificatePath, makeContextHolder)
      else
        makeContextHolder
    }

    val engine: SSLEngine = linker.sslEngineCtor.newInstance(
      contextHolder,
      bufferPool
    ).asInstanceOf[SSLEngine]

    Some(new Engine(engine, true))
  }
}
