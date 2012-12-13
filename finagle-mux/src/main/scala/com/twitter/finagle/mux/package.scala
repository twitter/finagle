package com.twitter.finagle

/**

Package mux implements a generic RPC multiplexer with a rich protocol.
Mux is itself encoding independent, so it is meant to use as the
transport for other RPC systems (eg. thrift). In OSI terminology, it
is a pure session layer.

In the below description, all numeric values are unsigned and in
big-endian byte order. The schema [4]size[10]body defines the field
size to be 4 bytes, followed by 10 of the field body. When symbols are
used to specify size, they refer to a previously defined field, eg.
''[4]size[size-4]body'' means 4 bytes of size specified by size-4
bytes of body, where size is interpreted as a 32-bit big-endian
integer. Constructions in parentheses followed by a ''*'' indicate
repeated values. If the ''*'' is followed by a name, the parenthesized
construct is repeated exactly this number of times. ''[s]foo'' is
shorthand for ''[4]size[size-4]foo'' where ''foo'' is a string. All
strings are Utf-8 encoded.

=Message framing=

Messages in mux are framed with a 4-byte big-endian size header,
followed by 1 byte describing the message type and a 3-byte tag; or,
diagrammatically: ''[4]size[1]type[3]tag''. The remainder of the frame
(rem=size-4 bytes) contains the body. Its format depends on the
message type, documented below.

The tag's MSB is reserved for future use, making its current namespace
23 bits. Tag 0 designates a "marker" T message that expects no reply.
The tag is otherwise arbitrary, and is chosen by the sender of the T
message.

Message-type numbering scheme is as follows. Message type 0 is
reserved for future use. T messages have bit 7 (the MSB) set to 1, R
messages set bit 7 to 0. Control messages set bit 6 to 1 and
application messages set it to 0. This distinction allows proxies to
be oblivious to parts of the protocol.

The protocol is full duplex: both the server and client may send T
messages initiating an exchange.

=Exchanges=

Messages are designated as "T messages" or "R messages", T and R being
stand-ins for transmit and receive. A T message initiates an exchange
and is assigned a free tag by the sender. A reply is either an R
message of the same type (Rx replies to Tx for some x), or an Rerr,
indicating a session layer error. R messages are matched to their T
messages by tag, and the reply concludes the exchange and frees the
tag for future use. Implementations should reuse small tag numbers.

=Messages=

''[4]size[1]Tinit[3]tag[2]version([s]key[s]value)*'' reinitializes a
session. Clients typically send this at the beginning of the session.
When doing so, the sender may issue no more T messages until the
corresponding ''[4]size[1]Rinit[2]version([s]key[s]value)'' has been
received. After the Rinit was received, all connection state has been
reset (outstanding tags are invalidated) and the stream is resumed
according to the newly negotiated parameters. Prior to the first
Tinit, the session operates at version 1. Rinit's version field is the
accepted version of the session (which may be lower than the one
requested by Tinit).

''[4]size[1]Treq[3]tag[1]nkeys([1]key[1]vsize[vsize]value)*nkeys[rem]body''
initiates the request described by its body. The request body is
delivered to the application. The request header contains a number of
key-value pairs that describe request metadata. Keys are:

 1. ''traceid'': a 24-byte value describing the full
[[http://research.google.com/archive/papers/dapper-2010-1.pdf Dapper]] trace id
assigned by the client. The value's format is ''[8]spanid[8]parentid[8]traceid''.

 1. ''traceflag'': a bitmask describing trace flags. Currently, the
only defined flag is bit 0 which enables "debug mode", asking the
server to force trace sampling.

''[4]size[1]Rreq[1]status[size-6]body'' replies to a request. Status
codes are as follows: 0=OK; the body contains the reply. 1=ERROR; the
body contains a string describing the error. 2=NACK; a negative
acknowledgment, the body contains a string describing the reason.

''[4]size[1]Rerr[n]why'' indicates that the corresponding T message
produced an error. Rerr is specifically for server errors: the server
failed to interpret or act on the message. The body carries a string
describing the error.

''[4]size[1]Tdrain[3]tag'' is a request sent by the server telling the
client to stop sending new requests. A client acknowledges this with
an Rdrain message.

''[4]size[1]Tping'' is sent by either party to check the liveness of
its peer; these should be responded to immediately with a Rping
message.

''[4]size[1]Tdiscarded[3]tag[size-8]why'' is a marker message
indicating that the Treq with the given tag has been discarded by the
client. This can be used as a hint for early termination. Why is a
string describing why the request was discarded. Note that it does
*not* free the server from the obligation of replying to the original
Treq.

 */
package object mux
