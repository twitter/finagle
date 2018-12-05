package com.twitter.finagle.client

import com.twitter.finagle.{Addr, Dtab}
import com.twitter.util.{Closable, Var, Witness}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

/**
 * For all paths registered to a given client and Dtab, the [[EndpointRegistry]]
 * keeps a reference to observe changes to the current weight and endpoints. A path
 * and Var[Addr] are added for a given client and Dtab by calling
 * `addObservation`. A path is removed for a given client and Dtab by calling
 * `removeObservation`.
 */
private[twitter] object EndpointRegistry {

  private type Observation = (AtomicReference[Addr], Closable)

  private type EndpointMap = mutable.Map[String, Observation]
  private type DtabMap = mutable.Map[Dtab, EndpointMap]

  val registry = new EndpointRegistry()
}

private[twitter] class EndpointRegistry {
  import EndpointRegistry._

  // synchronized on `this`
  private[this] val registry = mutable.Map.empty[String, DtabMap]

  /**
   * Returns a map of Dtabs to a map of paths to addrs for a given client
   *
   * @param client Name of the client
   */
  def endpoints(client: String): Map[Dtab, Map[String, Addr]] = synchronized {
    registry.get(client) match {
      case Some(dtabMap) =>
        dtabMap.mapValues { paths =>
          paths.mapValues { case (observation, _) => observation.get() }.toMap
        }.toMap
      case None => Map.empty
    }
  }

  /**
   * Register a collection of endpoints for a given client, Dtab, and path.
   * If the path already exits for the given client and Dtab, it is replaced.
   *
   * @param client Name of the client
   * @param dtab Dtab for this path
   * @param path Path to observe endpoints for
   * @param endpoints Collection of endpoints for this serverset
   */
  def addObservation(client: String, dtab: Dtab, path: String, endpoints: Var[Addr]): Unit = {
    val ar: AtomicReference[Addr] = new AtomicReference()
    val closable = endpoints.changes.register(Witness(ar))
    val observation = (ar, closable)
    synchronized {
      registry.get(client) match {
        case Some(dtabMap) =>
          dtabMap.get(dtab) match {
            case Some(dtabEntry) =>
              // If the path already exists, replace it and close the observation
              val prev = dtabEntry.put(path, observation)
              prev.foreach { case (_, closable) => closable.close() }
            case None =>
              dtabMap += ((dtab, mutable.Map(path -> observation)))
          }
        case None =>
          val endpointMap: EndpointMap = mutable.Map(path -> observation)
          val dtabMap: DtabMap = mutable.Map(dtab -> endpointMap)
          registry.put(client, dtabMap)
      }
    }
  }

  /**
   * Deregister a dtab and path to observe for a given client. If, after removal,
   * there are no paths for a dtab, remove the dtab from the client's registry
   * entry. If, after removal, there are no dtabs for the client, remove the
   * client from the registry.
   *
   * @param client Name of the client
   * @param dtab Dtab to remove the path for
   * @param path Path to remove observation for
   */
  def removeObservation(client: String, dtab: Dtab, path: String) = synchronized {
    registry.get(client).foreach { dtabEntries =>
      dtabEntries.get(dtab).foreach { entry =>
        entry.remove(path).foreach { case (_, closable) => closable.close() }
        if (entry.isEmpty) {
          dtabEntries.remove(dtab)
          if (dtabEntries.isEmpty) {
            registry.remove(client)
          }
        }
      }
    }
  }
}
