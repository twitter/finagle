/*
		CHANGING THIS FILE REQUIRES MANUAL REGENERATION

E.g.:

thrift --gen java tracing.thrift
cd gen-java
find . -type f -print0 | cpio -pmud0 ../../java

*/

/**
 * Define thrift structs for thrift request tracing.
 */

namespace java com.twitter.finagle.thrift.thrift
#@namespace scala com.twitter.finagle.thrift.thriftscala
namespace rb FinagleThrift


#************** Annotation.value **************
/**
 * The client sent ("cs") a request to a server. There is only one send per
 * span. For example, if there's a transport error, each attempt can be logged
 * as a WIRE_SEND annotation.
 *
 * If chunking is involved, each chunk could be logged as a separate
 * CLIENT_SEND_FRAGMENT in the same span.
 *
 * Annotation.host is not the server. It is the host which logged the send
 * event, almost always the client. When logging CLIENT_SEND, instrumentation
 * should also log the SERVER_ADDR.
 */
const string CLIENT_SEND = "cs"
/**
 * The client received ("cr") a response from a server. There is only one
 * receive per span. For example, if duplicate responses were received, each
 * can be logged as a WIRE_RECV annotation.
 *
 * If chunking is involved, each chunk could be logged as a separate
 * CLIENT_RECV_FRAGMENT in the same span.
 *
 * Annotation.host is not the server. It is the host which logged the receive
 * event, almost always the client. The actual endpoint of the server is
 * recorded separately as SERVER_ADDR when CLIENT_SEND is logged.
 */
const string CLIENT_RECV = "cr"
const string CLIENT_RECV_ERROR = "Client Receive Error"
/**
 * The server sent ("ss") a response to a client. There is only one response
 * per span. If there's a transport error, each attempt can be logged as a
 * WIRE_SEND annotation.
 *
 * Typically, a trace ends with a server send, so the last timestamp of a trace
 * is often the timestamp of the root span's server send.
 *
 * If chunking is involved, each chunk could be logged as a separate
 * SERVER_SEND_FRAGMENT in the same span.
 *
 * Annotation.host is not the client. It is the host which logged the send
 * event, almost always the server. The actual endpoint of the client is
 * recorded separately as CLIENT_ADDR when SERVER_RECV is logged.
 */
const string SERVER_SEND = "ss"
const string SERVER_SEND_ERROR = "Server Send Error"
/**
 * The server received ("sr") a request from a client. There is only one
 * request per span.  For example, if duplicate responses were received, each
 * can be logged as a WIRE_RECV annotation.
 *
 * Typically, a trace starts with a server receive, so the first timestamp of a
 * trace is often the timestamp of the root span's server receive.
 *
 * If chunking is involved, each chunk could be logged as a separate
 * SERVER_RECV_FRAGMENT in the same span.
 *
 * Annotation.host is not the client. It is the host which logged the receive
 * event, almost always the server. When logging SERVER_RECV, instrumentation
 * should also log the CLIENT_ADDR.
 */
const string SERVER_RECV = "sr"
/**
 * Optionally logs an attempt to send a message on the wire. Multiple wire send
 * events could indicate network retries. A lag between client or server send
 * and wire send might indicate queuing or processing delay.
 */
const string WIRE_SEND = "ws"
/**
 * Optionally logs an attempt to receive a message from the wire. Multiple wire
 * receive events could indicate network retries. A lag between wire receive
 * and client or server receive might indicate queuing or processing delay.
 */
const string WIRE_RECV = "wr"
const string WIRE_RECV_ERROR = "Wire Receive Error"
/**
 * Optionally logs progress of a (CLIENT_SEND, WIRE_SEND). For example, this
 * could be one chunk in a chunked request.
 */
const string CLIENT_SEND_FRAGMENT = "csf"
/**
 * Optionally logs progress of a (CLIENT_RECV, WIRE_RECV). For example, this
 * could be one chunk in a chunked response.
 */
const string CLIENT_RECV_FRAGMENT = "crf"
/**
 * Optionally logs progress of a (SERVER_SEND, WIRE_SEND). For example, this
 * could be one chunk in a chunked response.
 */
const string SERVER_SEND_FRAGMENT = "ssf"
/**
 * Optionally logs progress of a (SERVER_RECV, WIRE_RECV). For example, this
 * could be one chunk in a chunked request.
 */
const string SERVER_RECV_FRAGMENT = "srf"

#***** BinaryAnnotation.key where value = [1] and annotation_type = BOOL ******
/**
 * Indicates a client address ("ca") in a span. Most likely, there's only one.
 * Multiple addresses are possible when a client changes its ip or port within
 * a span.
 */
const string CLIENT_ADDR = "ca"
/**
 * Indicates a server address ("sa") in a span. Most likely, there's only one.
 * Multiple addresses are possible when a client is redirected, or fails to a
 * different server ip or port.
 */
const string SERVER_ADDR = "sa"

/**
 * Indicates the network context of a service recording an annotation with two
 * exceptions.
 *
 * When a BinaryAnnotation, and key is CLIENT_ADDR or SERVER_ADDR,
 * the endpoint indicates the source or destination of an RPC. This exception
 * allows zipkin to display network context of uninstrumented services, or
 * clients such as web browsers.
 */
struct Endpoint {
  // IPv4 host address packed into 4 bytes.
  // Ex for the ip 1.2.3.4, it would be (1 << 24) | (2 << 16) | (3 << 8) | 4
  1: i32 ipv4
  // IPv4 port
  // Note: this is to be treated as an unsigned integer, so watch for negatives.
  2: i16 port
  // Service name, such as "memcache" or "zipkin-web"
  // Note: Some implementations set this to "Unknown"
  3: string service_name
}

/**
 * An annotation is similar to a log statement. It includes a host field which
 * allows these events to be attributed properly, and also aggregatable.
 */
struct Annotation {
  1: i64 timestamp                 // microseconds from epoch
  2: string value                  // what happened at the timestamp?
  /**
   * Always the host that recorded the event. By specifying the host you allow
   * rollup of all events (such as client requests to a service) by IP address.
   */
  3: optional Endpoint host
  // don't reuse 4: optional i32 OBSOLETE_duration         // how long did the operation take? microseconds
}

enum AnnotationType { BOOL, BYTES, I16, I32, I64, DOUBLE, STRING }

/**
 * Binary annotations are tags applied to a Span to give it context. For
 * example, a binary annotation of "http.uri" could the path to a resource in a
 * RPC call.
 *
 * Binary annotations of type STRING are always queryable, though more a
 * historical implementation detail than a structural concern.
 *
 * Binary annotations can repeat, and vary on the host. Similar to Annotation,
 * the host indicates who logged the event. This allows you to tell the
 * difference between the client and server side of the same key. For example,
 * the key "http.uri" might be different on the client and server side due to
 * rewriting, like "/api/v1/myresource" vs "/myresource. Via the host field,
 * you can see the different points of view, which often help in debugging.
 */
struct BinaryAnnotation {
  1: string key,
  2: binary value,
  3: AnnotationType annotation_type,
  /**
   * The host that recorded tag, which allows you to differentiate between
   * multiple tags with the same key. There are two exceptions to this.
   *
   * When the key is CLIENT_ADDR or SERVER_ADDR, host indicates the source or
   * destination of an RPC. This exception allows zipkin to display network
   * context of uninstrumented services, or clients such as web browsers.
   */
  4: optional Endpoint host
}

/**
 * A trace is a series of spans (often RPC calls) which form a latency tree.
 *
 * The root span is where trace_id = id and parent_id = Nil. The root span is
 * usually the longest interval in the trace, starting with a SERVER_RECV
 * annotation and ending with a SERVER_SEND.
 */
struct Span {
  1: i64 trace_id                  // unique trace id, use for all spans in trace
  3: string name,                  // span name, rpc method for example
  4: i64 id,                       // unique span id, only used for this span
  5: optional i64 parent_id,                // parent span id
  6: list<Annotation> annotations, // list of all annotations/events that occurred, sorted by timestamp
  8: list<BinaryAnnotation> binary_annotations, // any binary annotations
  9: bool debug,                  // if true, we DEMAND that this span passes all samplers
  10: optional i64 timestamp      // the time at which the span was generated
  11: optional i64 duration       // measurement in microseconds of the critical path, if known
  12: optional i64 trace_id_high  // the optional high 64 bits of a 128 bit identifier
                                  // (must be non-zero if present)
}


/**
 * At connection time, we can let the server know who we are so
 * they can book keep and optionally reject unknown clients.
 */
struct ClientId {
  1: string name
}

/**
 * This struct serializes com.twitter.finagle.Context
 */
struct RequestContext {
  1: binary key,
  2: binary value
}

/**
 * Serializes an individual delegation.
 */
struct Delegation {
  1: string src
  2: string dst
}

/**
 * The following are for finagle-thrift specific tracing headers &
 * negotiation.
 */

/**
 * RequestHeader defines headers for the request. These carry the span data, and
 * a flag indicating whether the request is to be debugged.
 */
struct RequestHeader {
  1: i64 trace_id,
  2: i64  span_id,
  3: optional i64 parent_span_id,
  5: optional bool sampled // if true we should trace the request, if not set we have not decided.
  6: optional ClientId client_id
  7: optional i64 flags // contains various flags such as debug mode on/off
  8: list<RequestContext> contexts

  // Support for destination (partially resolved names) and delegation tables.
  9: optional string dest
  10: optional list<Delegation> delegations
  11: optional i64 trace_id_high
}

/**
 * The Response carries a reply header for tracing. These are
 * empty unless the request is being debugged, in which case a
 * transcript is copied.
 */
struct ResponseHeader {
  1: list<Span> spans
  2: list<RequestContext> contexts
}

/**
 * These are connection-level options negotiated during protocol
 * upgrade.
 */
struct ConnectionOptions {
}

/**
 * This is the struct that a successful upgrade will reply with.
 */
struct UpgradeReply {
}
