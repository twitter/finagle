package com.twitter.finagle

import com.twitter.finagle.util.LoadService
import com.twitter.util.{Closable, Future, Time}
import java.net.{InetSocketAddress, SocketAddress}
import java.util.logging.Logger
import scala.collection.mutable

trait Announcement extends Closable {
  def close(deadline: Time) = unannounce()
  def unannounce(): Future[Unit]
}

trait ProxyAnnouncement extends Announcement with Proxy {
  val forums: List[String]
}

trait Announcer {
  val scheme: String
  def announce(addr: InetSocketAddress, name: String): Future[Announcement]
}

class AnnouncerNotFoundException(scheme: String)
  extends Exception("Announcer not found for scheme \"%s\"".format(scheme))

class AnnouncerForumInvalid(forum: String)
  extends Exception("Announcer forum \"%s\" is not valid".format(forum))

class MultipleAnnouncersPerSchemeException(announcers: Map[String, Seq[Announcer]])
  extends NoStacktrace
{
  override def getMessage = {
    val msgs = announcers map { case (scheme, rs) =>
      "%s=(%s)".format(scheme, rs.map(_.getClass.getName).mkString(", "))
    } mkString(" ")
    "Multiple announcers defined: %s".format(msgs)
  }
}

object Announcer {
  private[this] lazy val announcers = {
    val announcers = LoadService[Announcer]()
    val log = Logger.getLogger(getClass.getName)

    val dups = announcers groupBy(_.scheme) filter { case (_, rs) => rs.size > 1 }
    if (dups.size > 0) throw new MultipleAnnouncersPerSchemeException(dups)

    for (r <- announcers)
      log.info("Announcer[%s] = %s(%s)".format(r.scheme, r.getClass.getName, r))
    announcers
  }

  def get[T <: Announcer](clazz: Class[T]): Option[T] =
    announcers find { _.getClass isAssignableFrom clazz } map { _.asInstanceOf[T] }

  private[this] val _announcements = mutable.Set[(InetSocketAddress, List[String])]()
  def announcements = synchronized { _announcements.toSet }

  def announce(addr: InetSocketAddress, forum: String): Future[Announcement] = {
    val announcement = forum.split("!", 2) match {
      case Array(scheme, name) =>
        announcers.find(_.scheme == scheme) match {
          case Some(announcer) => announcer.announce(addr, name)
          case None => Future.exception(new AnnouncerNotFoundException(scheme))
        }

      case _ =>
        Future.exception(new AnnouncerForumInvalid(forum))
    }

    announcement map { ann =>
      val lastForums = ann match {
        case a: ProxyAnnouncement => a.forums
        case _ => Nil
      }

      val proxyAnnouncement = new ProxyAnnouncement {
        val self = ann
        def unannounce() = ann.unannounce()
        val forums = forum :: lastForums
      }

      synchronized {
        _announcements -= ((addr, lastForums))
        _announcements += ((addr, proxyAnnouncement.forums))
      }

      proxyAnnouncement
    }
  }
}
