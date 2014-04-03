package com.twitter

import com.twitter.app.GlobalFlag
import com.twitter.finagle.serverset2.Zk2Resolver
import com.twitter.finagle.zookeeper.ZkResolver
import com.twitter.finagle.{Addr, NameTree, Namer, Resolver, Path, WeightedInetSocketAddress}
import com.twitter.util.Activity
import java.net.InetSocketAddress

object newZk extends GlobalFlag(
  false, 
  "Use the new ZooKeeper implementation "+
  "for /$/com.twitter.serverset")

/**
 * The serverset namer takes paths of the form
 *
 *    hosts/path...
 *
 * and returns a dynamic name representing its resolution into a tree
 * of /$/inet names.
 */
class serverset extends Namer {
  val zk = {
    val cls = if(newZk()) classOf[Zk2Resolver] else classOf[ZkResolver]
    val Some(z) = Resolver.get(cls)
    z
  }

  def lookup(path: Path): Activity[NameTree[Path]] = path match {
    case Path.Utf8(hosts, rest@_*) =>
      val va = zk.bind(hosts+"!/"+(rest mkString "/"))

      Activity(va map {
        case Addr.Bound(addrs) =>
          val leaves = addrs.toSeq collect {
            // This filters out non inet-addresses. This could be
            // surprising if you are converting names that produce
            // esoteric socket addresses. There's no real clean solution
            // since we can't assign these into a global namespace.
            case ia: InetSocketAddress =>
              NameTree.Leaf(Path.Utf8("$", "inet", ia.getHostName, ia.getPort.toString))

            // Temporary hack until we add first-class weights back
            // into NameTrees.
            case WeightedInetSocketAddress(ia, w) =>
              NameTree.Leaf(Path.Utf8("$", "inetw", w.toString, ia.getHostName, ia.getPort.toString))
          }

          Activity.Ok(NameTree.Union(leaves:_*))

        case Addr.Neg => Activity.Ok(NameTree.Neg)
        case Addr.Pending => Activity.Pending
        case Addr.Failed(exc) => Activity.Failed(exc)
      })

    case _ =>
      Activity.exception(new Exception("Invalid com.twitter.namer path "+path.show))
  }
}
