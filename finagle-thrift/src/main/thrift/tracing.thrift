/**
 * Define thrift structs for thrift request tracing.
 */

namespace java com.twitter.finagle.thrift.thrift

/**
 * The following is from BigBrotherBird:
 *   http://j.mp/fZZnyD
 */

// these are the annotations we always expect to find in a span
const string CLIENT_SEND = "cs"
const string CLIENT_RECV = "cr"
const string SERVER_SEND = "ss"
const string SERVER_RECV = "sr"

// this represents a host and port in a network
struct Endpoint {
  1: optional i32 ipv4,
  2: optional i16 port                      // beware that this will give us negative ports. some conversion needed
  3: optional string service_name           // which service did this operation happen on?
}

// some event took place, either one by the framework or by the user
struct Annotation {
  1: optional i64 timestamp                 // microseconds from epoch
  2: optional string value                  // what happened at the timestamp?
  3: optional Endpoint host                 // host this happened on
}

struct Span {
  1: optional i64 trace_id                  // unique trace id, use for all spans in trace
  3: optional string name,                  // span name, rpc method for example
  4: optional i64 id,                       // unique span id, only used for this span
  5: optional i64 parent_id,                // parent span id
  6: optional list<Annotation> annotations, // list of all annotations/events that occured
  7: optional map<string, binary> binary_annotations // any binary annotations
}

/**
 * The following are for finagle-thrift specific tracing headers &
 * negotiation.
 */

/**
 * TracedRequest defines trace headers. These carry the span data, and
 * a flag indicating whether the request is to be debugged.
 */
struct TracedRequestHeader {
  1: i64 trace_id,
  2: i64  span_id,
  3: optional i64 parent_span_id,
  4: bool debug,
  5: optional bool sampled // if true we should trace the request, if not set we have not decided.
}

/**
 * The TracedResponse carries a reply header for tracing. These are
 * empty unless the request is being debugged, in which case a
 * transcript is copied.
 */
struct TracedResponseHeader {
  1: list<Span> spans
}

/**
 * These are connection-level trace options negotiated during protocol
 * upgrade. (Intentionally left blank: for future use).
 */
struct TraceOptions {}
