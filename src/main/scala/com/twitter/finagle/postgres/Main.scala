package com.twitter.finagle.postgres

import com.twitter.logging.Logger
import com.twitter.util.Future

//import com.twitter.util.Future
//import protocol.Communication

case class User(email: String, name: String)

object Main {
  private val logger = Logger(getClass.getName)

  def main(args: Array[String]) {
    import com.twitter.logging.config._

    val config = new LoggerConfig {
      node = ""
      level = Logger.DEBUG
      handlers = new ConsoleHandlerConfig {
      }
    }
    config()

    val client = Client("localhost:5432", "mkhadikov", Some("pass"), "contacts")

    val f: Future[QueryResponse] = client.prepare("select * from users where email = $1").flatMap {
      ps =>
        ps.fire("mickey@mouse.com")
    }
    logger.ifDebug("Rows " + f.get)

//    val fp = client.send(Communication.request(new Parse(name = "1", query = "select * from users"), flush = true)) {
//      case a => Future.value(a)
//    }

//    logger.ifDebug("Responded " + fp.get)
    //    val f = client.select("select * from users") { row =>
    //      User(row.getString("email"), row.getString("name"))
    //    }
    //
    //    logger.debug("Responded " + f.get)

//    val fb = client.send(Communication.request(new Bind(portal = "1", name = "1"), flush = true)) {
//      case b => Future.value(b)
//    }
//
//    logger.ifDebug("Responded " + fb.get)
//
//    val fd = client.send(Communication.request(new Describe(portal = true, name = "1"), flush = true)) {
//      case b => Future.value(b)
//    }
//
//    logger.ifDebug("Responded " + fd.get)

//
//    val fe = client.send(Communication.request(new Execute(name = "1", maxRows = 2), flush = true)) {
//      case b => Future.value(b)
//    }
//
//    fe.get
//
//    val fe2 = client.send(Communication.request(new Execute(name = "1", maxRows = 2), flush = true)) {
//      case b => Future.value(b)
//    }
//
//    fe2.get
//
//    println("TODO")
//
//    //    val fs = client.send(Communication.request(Sync, flush = true)) {
//    //      case b => Future.value(b)
//    //    }
//    //
//    //    fs.get
//
//    //    client.send(Communication.requestAndFlush(new Describe(false))) {
//    //      case b => Future.value(b)
//    //    }
//
//    val f = client.select("select * from users") { row =>
//      User(row.getString("email"), row.getString("name"))
//    }
//    //
//    logger.debug("Responded " + f.get)
//
//    //
    //    val fi = client.executeUpdate("insert into users(email, name) values ('mickey@mouse.com', 'Mickey Mouse')," +
    //      " ('bugs@bunny.com', 'Bugs Bunny')")
    //
    //    logger.debug("Responded " + fi.get)
    //
    //    val fd = client.executeUpdate("delete from users where name = 'Mickey Mouse'")
    //
    //    logger.debug("Responded " + fd.get)
    //
    //    val fu = client.executeUpdate("update users set email = 'bugs@bunny.org' where name = 'Bugs Bunny'")
    //
    //    logger.debug("Responded " + fu.get)
    //
    //    client.close()

  }

}
