A MySQL client built for finagle.

---
## Overview

*This is meant to give a very brief overview of the MySQL Client/Server protocol and reference relevant source code within finagle-mysql. For an exposition of the MySQL Client/Server protocol, refer to [MySQL Documentation](http://dev.mysql.com/doc/internals/en/client-server-protocol.html)
and [Understanding MySQL Internal](http://my.safaribooksonline.com/book/databases/mysql/0596009577/client-server-communication/orm9780596009571-chp-4).*

**Packets** - The basic unit of communication for the MySQL Client/Server protocol is an application-layer packet. A MySQL packet consists of a header (size and sequence number) followed by a body. Packets can be fragmented across frames during transmission. To simplify the decoding of results received from the server, the codec includes a packet frame decoder on the pipeline.

`* protocol/Packet.scala, codec/PacketFrameDecoder.scala`

**Handshake and Authentication** - When a client connects to the server, the server sends a greeting. This greeting contains information about the server version, protocol, capabilities, etc. The client replies with a login request containing, among other information, authentication credentials. To ensure connections are authenticated before they are issued by finagle, the codec implements prepareConnFactory with an AuthenticationProxy.

`* protocol/Handshake.scala, Codec.scala`

**Capabilities** - The client and server express their capability succinctly as bit vectors. Each set bit in the vector represents what the client/server is capable of or willing to do. Finagle-mysql provides constants of all the capability flags available (at the time of writing) and a comprehensive way to build capability bit vectors.

    // This clients capabilities
    val clientCapability = Capability(
      LongFlag,
      Transactions,
      Protocol41,
      FoundRows,
      Interactive,
      LongPassword,
      ConnectWithDB,
      SecureConnection,
      LocalFiles
    )
`* protocol/Capability.scala, Codec.scala`

 Note: This client only supports protocol version 4.1 (available in MySQL version 4.1 and above). This is strictly enforced during authentication with a MySQL server.

**Requests** - Most requests sent to the server are Command Requests. They contain a command byte and any arguments that are specific to the command. Command Requests are sent to the server as a MySQL Packet. The first byte of the packet body must contain a valid command byte followed by the arguments. Within finagle-mysql, each Request object has a data field which defines the body of the Packet. Requests are translated into a logical MySQL Packet by the toChannelBuffer method when they reach the Encoder.

`* protocol/Request.scala`

**Results** - finagle-mysql translates packets received from the server into Scala objects. Each result object has a relevant decode method that translates the packet(s) into the object according to the protocol. Result packets can be distinguished by their first byte. Some result packets denote the start of a longer transmission and need to be defragged by the decoder.

ResultSets are returned from the server for queries that return Rows. A Row can be String encoded or Binary encoded depending on the Request used to execute the query. For example, a QueryRequest uses the String protocol and a PreparedStatement uses the binary protocol.

`* codec/Endec.scala, protocol/{Result.scala, ResultSet.scala, PreparedStatement.scala}`

**Value** - finagle-mysql provides a Value ADT that can represent all values returned from MySQL. However, this does not include logic to decode every data type. For unsupported values finagle-mysql will return a RawStringValue and RawBinaryValue for the String and Binary protocols, respectively. Other note worthy Value objects include NullValue (SQL NULL) and EmptyValue.

The following code depicts a robust, safe, and idiomatic way to extract and deconstruct a Value from a Row.

    // The row.valueOf(...) method returns an Option[Value].
    val userId: Option[Long] = row.valueOf("id") map {
      case LongValue(id) => id
      case _ => -1
    }

Pattern matching all possible values of the Value ADT gives great flexibility and control. For example, it allows the programmer to handle NullValues and EmptyValues with specific application logic.

`* protocol/Value.scala`

**Byte Buffers** - The BufferReader and BufferWriter interfaces provide convenient methods for reading/writing primitive data types exchanged between the client/server. This includes all primitive numeric types and strings (null-terminated and length coded). All Buffer methods are side-effecting, that is, each call to a read*/write* method will increase the current read and write position. Note, the bytes exchanged between the client/server are encoded in little-endian byte order.

`* protocol/Buffer.scala`

**Charset** - Currently, finagle-mysql only supports UTF-8 character encodings. This is strictly enforced when authenticating with a MySQL server. For more information about how to configure a MySQL instance to use UTF-8 refer to the [MySQL Documentation](http://dev.mysql.com/doc/refman/5.0/en/charset-applications.html).

Note: MySQL also supports variable charsets at the table and field level. This charset data is part of the field packet.

`* protocol/Charset.scala`