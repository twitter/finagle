A MySQL client built for finagle.

---
## Protocol Overview  

*This is meant to give a very brief overview of the MySQL Client/Server protocol and reference relevant source files within finagle-mysql. For a more detailed exposition of the MySQL Client/Server protocol, refer to [MySQL Forge](http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol)
and [Understanding MySQL Internal](http://my.safaribooksonline.com/book/databases/mysql/0596009577/client-server-communication/orm9780596009571-chp-4).*

**Packets** - The basic unit of communication for the MySQL Client/Server protocol is an application-layer packet. A MySQL packet consists of a header (size and sequence number) followed by a body. Packets can be fragmented across frames during transmission. To simplify the decoding of results received from the server, the codec includes a packet frame decoder on the pipeline.

    * protocol/Packet.scala, codec/PacketFrameDecoder.scala

**Handshake and Authentication** - When a client connects to the server, the server sends a greeting. This greeting contains information about the server version, protocol, capabilities, etc. The client replies with a login request containing, among other information, authentication credentials. To ensure connections are authenticated before they are issued by finagle, the codec implements prepareConnFactory with an AuthenticationProxy.

    * protocol/Handshake.scala, Codec.scala

**Capabilities** - The client and server express their capability succinctly as bit vectors. Each set bit in the vector represents what the client/server is capable of or willing to do. Finagle-mysql provides constants of all the capability flags available (at the time of writing) and a comprehensive way to build capability bit vectors.

    // This clients capabilities
		private[this] val clientCapability = 
		  Capability(LongFlag, 
		             Transactions, 
		             Protocol41, 
		             FoundRows, 
		             Interactive, 
		             LongPassword, 
		             ConnectWithDB, 
		             SecureConnection, 
		             LocalFiles)
    * protocol/Capability.scala, Codec.scala
 
 Note: This client only supports protocol version 4.1 and above. This is strictly enforced during authentication with a MySQL server.

**Requests** - Most requests sent to the server are Command Requests. They contain a command byte and any arguments that are specific to the command. Command Requests are sent to the server as a MySQL Packet. The first byte of the packet body must contain a valid command byte followed by the arguments. Within finagle-mysql, each Request object has a data field which defines the body of the Packet. Requests are translated into a logical MySQL Packet by the toChannelBuffer method when they reach the Encoder.

    * protocol/Request.scala

**Results** - finagle-mysql translates packets received from the server into Scala objects. Each result object has a relevant decode method that translates the packet(s) into the object according to the protocol. Result packets can be distinguished by their first byte. Some result packets denote the start of a longer transmission and need to be defragged by the decoder.

    * codec/Endec.scala, protocol/{Result.scala, ResultSet.scala, PreparedStatement.scala}

**Buffers** - The BufferReader and BufferWriter class provide convenient methods for reading/writing common data types exchanged between the client/server. Note, data exchanged between the client/server is encoded in little-endian byte order.

    * protocol/Buffer.scala