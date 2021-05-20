package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}

/**
 * Package mux implements a generic RPC multiplexer with a rich protocol.
 * Mux is itself encoding independent, so it is meant to use as the
 * transport for other RPC systems (eg. thrift). In OSI terminology, it
 * is a pure session layer.
 *
 * In the below description, all numeric values are unsigned and in
 * big-endian byte order. The schema ''size:4 body:10'' defines the
 * field size to be 4 bytes, followed by 10 bytes of the field body. The
 * schema ''key~4'' defines the field key to be defined by 4 bytes
 * interpreted as the size of the field, followed by that many bytes
 * comprising the field itself--it is shorthand for ''keysize:4 key:keysize''.
 * Groups are denoted by parenthesis; ''*'' denotes repetition of the
 * previous schema 0 or more times, while `{n}` indicates repetition
 * exactly ''n'' times. Unspecified sizes consume the rest of the frame:
 * they may be specified only as the last field in the message.
 *
 * All strings  in Mux are Utf-8 encoded, and are never null-terminated.
 *
 * =Message framing=
 *
 * Messages in mux are framed with a 4-byte big-endian size header,
 * followed by 1 byte describing the message type and a 3-byte tag; or,
 * diagrammatically: ''size:4 type:1 tag:3''. The remainder of the frame
 * (size-4 bytes) contains the body. Its format depends on the
 * message type, documented below.
 *
 * Tag 0 designates a "marker" T message that expects no reply. Some
 * messages may be split into an ordered sequence of fragments. Tag MSB=0
 * denotes the last message in such a sequence, making the tag namespace
 * 23 bits. The tag is otherwise arbitrary, and is chosen by the sender
 * of the T message.
 *
 * Currently, only Tdispatch and Rdispatch messages may be split into an
 * ordered sequence of fragments. TdispatchError message ends a Tdispatch
 * sequence and an Rerr ends an Rdispatch sequence.
 *
 * Message types, interpreted as a two's complement, 1-byte integer are
 * numbered as follows: positive numbers are T-messages; their negative
 * complement is the corresponding R message. T-messages greater than 63
 * (correspondingly R-messages smaller than -63) are session messages.
 * The message number -128 is reserved for Rerr. All other messages are
 * application messages. Middle boxes may forward application messages
 * indiscriminately. Because of an early implementation bug, two aliases
 * exist: 127 is Rerr, and -62 is Tdiscarded.
 *
 * The protocol is full duplex: both the server and client may send T
 * messages initiating an exchange.
 *
 * =Exchanges=
 *
 * Messages are designated as "T messages" or "R messages", T and R being
 * stand-ins for transmit and receive. A T message initiates an exchange
 * and is assigned a free tag by the sender. A reply is either an R
 * message of the same type (Rx replies to Tx for some x), or an Rerr,
 * indicating a session layer error. R messages are matched to their T
 * messages by tag, and the reply concludes the exchange and frees the
 * tag for future use. Implementations should reuse small tag numbers.
 *
 * =Messages=
 *
 * ''size:4 Tinit:1 tag:3 version:2 (key~4 value~4)*'' reinitializes a
 * session. Clients typically send this at the beginning of the session.
 * When doing so, the sender may issue no more T messages until the
 * corresponding ''size:4 Rinit:1 tag:3 version:2 (key~4 value~4)*'' has been
 * received. After the Rinit was received, all connection state has been
 * reset (outstanding tags are invalidated) and the stream is resumed
 * according to the newly negotiated parameters. Prior to the first
 * Tinit, the session operates at version 1. Rinit's version field is the
 * accepted version of the session (which may be lower than the one
 * requested by Tinit).
 *
 * ''size:4 Treq:1 tag:3 n:1 (key:1 value~1){n} body:'' initiates the
 * request described by its body. The request body is delivered to the
 * application. The request header contains a number of key-value pairs
 * that describe request metadata.
 *
 * Keys for ''Treq'' messages are as follows:
 *
 *  1. ''traceid'': a 24-byte value describing the full
 * [[https://research.google.com/archive/papers/dapper-2010-1.pdf Dapper]] trace id
 * assigned by the client. The value's format is ''spanid:8 parentid:8 traceid:8''.
 *
 *  2. ''traceflag'': a bitmask describing trace flags. Currently, the
 * only defined flag is bit 0 which enables "debug mode", asking the
 * server to force trace sampling.
 *
 * ''size:4 Tdispatch:1 tag:3 nctx:2 (ckey~2 cval~2){nc} dst~2 nd:2
 * (from~2 to~2){nd} body:'' implements ''destination dispatch''.
 * Tdispatch messages carry a set of keyed request contexts, followed by
 * a logical destination encoded as a UTF-8 string. A delegation table
 * follows describing rewrite rules that apply to this request.
 *
 * ''size:4 Rreq:1 tag:3 status:1 body:'' replies to a request. Status
 * codes are as follows: 0=OK; the body contains the reply. 1=ERROR; the
 * body contains a string describing the error. 2=NACK; a negative
 * acknowledgment, the body contains a string describing the reason.
 *
 * ''size:4 Rdispatch:1 tag:3 status:1 nctx:2 (key~2 value~2){nctx} body:'' replies
 * to a Tdispatch request. Status codes are as in Rreq. Replies can include
 * request contexts. MuxFailure flags are currently sent via Rdispatch contexts
 * under the "MuxFailure" key. See the MuxFailure flags section below.
 *
 * ''size:4 Rerr:1 tag:3 why:'' indicates that the corresponding T message
 * produced an error. Rerr is specifically for server errors: the server
 * failed to interpret or act on the message. The body carries a string
 * describing the error.
 *
 * ''size:4 Tdrain:1 tag:3'' is a request sent by the server telling the
 * client to stop sending new requests. A client acknowledges this with
 * an Rdrain message.
 *
 * ''size:4 Tping:1 tag:3'' is sent by either party to check the liveness of
 * its peer; these should be responded to immediately with a Rping
 * message.
 *
 * ''size:4 Tdiscarded:1 tag:3 discard_tag:3 why:'' is a marker message and therefore
 * has a tag value of 0. ''discard_tag'' indicates the tag of the Tdispatch to be
 * discarded by the client. This can be used as a hint for early termination. Why is
 * a string describing why the request was discarded. Note that it does *not* free
 * the server from the obligation of replying to the original Treq.
 *
 * ''size:4 Tlease:1 tag:3 unit:1 howmuch:8'' is a marker message indicating that a
 * lease has been issued for ''howmuch'' units. As a marker message, its tag value must
 * be 0. Unit '0' is reserved for duration in milliseconds. Whenever a lease has not been
 * issued, a client can assume it holds an indefinite lease. Adhering to the lease is
 * optional, but the server may reject requests or provide degraded service should the
 * lease expire. This is used by servers to implement features like garbage collection
 * avoidance.
 *
 * =MuxFailure Flags=
 *
 * Failure flags are read and written as an 8 byte integer. Unrecognized flags
 * will be ignored silently, but should all be considered reserved for future
 * use.
 *
 *  Flag          Value     Meaning
 *  Restartable   1 << 0    Request is safe to re-issue
 *  Rejected      1 << 1    Request was rejected/Nacked by the server
 *  NonRetryable  1 << 2    Request should not be retried
 *
 * =Security=
 *
 * TLS is supported via three mechanisms:
 * - Explicit and exclusive TLS. This pathway involves requiring the establishment of TLS
 *   immediately after establishing the socket connection. This is configured by adding TLS
 *   configuration to the client or server and __not__ configuring opportunistic TLS or
 *   TLS snooping (see below).
 *
 * - Negotiated Opportunistic TLS. This pathway involves starting the connection as cleartext
 *   and the client and server subsequently negotiate a TLS level via the handshake. Based on
 *   that handshake the connection is either left as cleartext or upgraded to TLS. This is
 *   configured by adding TLS configuration and also configuring an opportunistic TLS level
 *   but __not__ configuring TLS snooping.
 *
 *   In this pathway there are three configuration options:
 *   - `Off` signals that TLS is not supported by this peer
 *   - `Desired` signals that TLS is preferred but not required by this peer
 *   - `Required` signals that this peer will only allow the session to continue over TLS
 *
 * - TLS snooping. This pathway allows a server to use TLS either by performing a TLS
 *   handshake immediately after the socket is established or by starting the session as
 *   cleartext or using the negotiated pathway described above. If the session is started as
 *   a TLS session the headers that drive the opportunistic TLS pathway are ignored.
 *
 *   Note that the server may still require TLS but leaves the option to start TLS immediately
 *   after establishing the socket or starting cleartext and requiring TLS via the opportunistic
 *   TLS pathway described above.
 */
package object mux {

  /**
   * The name of the finagle-mux [[ToggleMap]].
   */
  private val LibraryName: String =
    "com.twitter.finagle.mux"

  /**
   * The [[ToggleMap]] used for finagle-mux.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
