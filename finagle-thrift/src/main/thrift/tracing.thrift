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

/**
 * The following is from BigBrotherBird:
 *   http://j.mp/fZZnyD
 */

// these are the annotations we always expect to find in a span
const string WIRE_SEND = "ws"
const string WIRE_RECV = "wr"
const string CLIENT_SEND = "cs"
const string CLIENT_RECV = "cr"
const string SERVER_SEND = "ss"
const string SERVER_RECV = "sr"
const string SERVER_ADDR = "sa"
const string CLIENT_ADDR = "ca"
const string CLIENT_SEND_FRAGMENT = "csf"
const string CLIENT_RECV_FRAGMENT = "crf"
const string SERVER_SEND_FRAGMENT = "ssf"
const string SERVER_RECV_FRAGMENT = "srf"

// this represents a host and port in a network
struct Endpoint {
  1: i32 ipv4,
  2: i16 port                      // beware that this will give us negative ports. some conversion needed
  3: string service_name           // which service did this operation happen on?
}

// some event took place, either one by the framework or by the user
struct Annotation {
  1: i64 timestamp                 // microseconds from epoch
  2: string value                  // what happened at the timestamp?
  3: optional Endpoint host        // host this happened on
  4: optional i32 duration         // how long did the operation take? microseconds
}

enum AnnotationType { BOOL, BYTES, I16, I32, I64, DOUBLE, STRING }

// tag this span with a key and a value of AnnotationType type.
struct BinaryAnnotation {
  1: string key,
  2: binary value,
  3: AnnotationType annotation_type,
  4: optional Endpoint host
}

struct Span {
  1: i64 trace_id                  // unique trace id, use for all spans in trace
  3: string name,                  // span name, rpc method for example
  4: i64 id,                       // unique span id, only used for this span
  5: optional i64 parent_id,                // parent span id
  6: list<Annotation> annotations, // list of all annotations/events that occured
  8: list<BinaryAnnotation> binary_annotations, // any binary annotations
  9: bool debug                    // if true, we DEMAND that this span passes all samplers
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
