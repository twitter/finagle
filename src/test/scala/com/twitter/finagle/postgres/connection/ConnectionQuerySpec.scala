package com.twitter.finagle.postgres.connection

import com.twitter.finagle.postgres.Spec
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values.Charsets
import org.jboss.netty.buffer.ChannelBuffers

class ConnectionQuerySpec extends Spec {
  "A postgres connection" should {
    "handle an empty query response" in {
      val connection = new Connection(Connected)

      connection.send(Query(""))
      connection.receive(EmptyQueryResponse)
      val SelectResult(fields, rows) = connection.receive(ReadyForQuery('I')).get

      fields.length mustEqual 0
      rows mustEqual List()
    }

    "handle a create table query" in {
      val connection = new Connection(Connected)

      connection.send(Query("create table ids"))
      connection.receive(CommandComplete(CreateTable))
      val response = connection.receive(ReadyForQuery('I'))

      response must equal(Some(CommandCompleteResponse(1)))
    }

    "handle a delete query" in {
      val connection = new Connection(Connected)

      connection.send(Query("delete from ids"))
      connection.receive(CommandComplete(Delete(2)))
      val response = connection.receive(ReadyForQuery('I'))

      response must equal(Some(CommandCompleteResponse(2)))

    }

    "handle an insert query" in {
      val connection = new Connection(Connected)

      connection.send(Query("insert into ids values (1)"))
      connection.receive(CommandComplete(Insert(1)))
      val response = connection.receive(ReadyForQuery('I'))

      response must equal(Some(CommandCompleteResponse(1)))

    }

    "handle an update query" in {
      val connection = new Connection(Connected)

      connection.send(Query("update ids set id = 2 where id = 1"))
      connection.receive(CommandComplete(Update(1)))
      val response = connection.receive(ReadyForQuery('I'))

      response must equal(Some(CommandCompleteResponse(1)))
    }

    "handle an empty select query" in {
      val connection = new Connection(Connected)

      connection.send(Query("select * from emails"))
      connection.receive(RowDescription(Array(FieldDescription("email",16728,2,1043,-1,-1,0))))
      connection.receive(CommandComplete(Select(0)))
      val SelectResult(fields, rows) = connection.receive(ReadyForQuery('I')).get
      assert(fields sameElements Array(Field("email", 0, 1043)))
      rows must equal (List())
    }

    "handle a select query" in {
      val connection = new Connection(Connected)

      val row1 = DataRow(Array(Some(ChannelBuffers.copiedBuffer("donald@duck.com".getBytes(Charsets.Utf8)))))
      val row2 = DataRow(Array(Some(ChannelBuffers.copiedBuffer("daisy@duck.com".getBytes(Charsets.Utf8)))))

      connection.send(Query("select * from emails"))
      connection.receive(RowDescription(Array(FieldDescription("email",16728,2,1043,-1,-1,0))))

      connection.receive(row1)
      connection.receive(row2)
      connection.receive(CommandComplete(Select(2)))
      val SelectResult(fields, rows) = connection.receive(ReadyForQuery('I')).get

      fields must contain theSameElementsAs Array(Field("email", 0, 1043))
      rows must equal (List(row1, row2))
    }
  }
}