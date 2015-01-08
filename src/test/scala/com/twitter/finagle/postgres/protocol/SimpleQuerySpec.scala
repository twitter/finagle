package com.twitter.finagle.postgres.protocol

import com.twitter.finagle.postgres.connection.Connected
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.Charsets
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

@RunWith(classOf[JUnitRunner])
class SimpleQuerySpec extends Specification with ConnectionSpec {

  "Postgres client" should {

    "Handle empty query response" inConnection {

      setState(Connected)

      send(Query(""))
      receive(EmptyQueryResponse)
      receive(ReadyForQuery('I'))

      response === Some(SelectResult(IndexedSeq(), List()))

    }

    "Handle create table query" inConnection {

      setState(Connected)

      send(Query("create table ids"))
      receive(CommandComplete(CreateTable))
      receive(ReadyForQuery('I'))

      response === Some(CommandCompleteResponse(1))

    }

    "Handle delete query" inConnection {

      setState(Connected)

      send(Query("delete from ids"))
      receive(CommandComplete(Delete(2)))
      receive(ReadyForQuery('I'))

      response === Some(CommandCompleteResponse(2))

    }

    "Handle insert query" inConnection {

      setState(Connected)

      send(Query("insert into ids values (1)"))
      receive(CommandComplete(Insert(1)))
      receive(ReadyForQuery('I'))

      response === Some(CommandCompleteResponse(1))

    }

    "Handle update query" inConnection {

      setState(Connected)

      send(Query("update ids set id = 2 where id = 1"))
      receive(CommandComplete(Update(1)))
      receive(ReadyForQuery('I'))

      response === Some(CommandCompleteResponse(1))

    }

    "Handle empty select query" inConnection {

      setState(Connected)

      send(Query("select * from emails"))
      receive(RowDescription(IndexedSeq(FieldDescription("email",16728,2,1043,-1,-1,0))))
      receive(CommandComplete(Select(0)))
      receive(ReadyForQuery('I'))

      response === Some(SelectResult(IndexedSeq(Field("email", 0, 1043)), List()))

    }

    "Handle select query" inConnection {
      val row1 = DataRow(IndexedSeq(ChannelBuffers.copiedBuffer("donald@duck.com".getBytes(Charsets.Utf8))))
      val row2 = DataRow(IndexedSeq(ChannelBuffers.copiedBuffer("daisy@duck.com".getBytes(Charsets.Utf8))))

      setState(Connected)

      send(Query("select * from emails"))
      receive(RowDescription(IndexedSeq(FieldDescription("email",16728,2,1043,-1,-1,0))))

      receive(row1)
      receive(row2)
      receive(CommandComplete(Select(2)))
      receive(ReadyForQuery('I'))

      response === Some(SelectResult(IndexedSeq(Field("email", 0, 1043)), List(row1, row2)))

    }

  }
}