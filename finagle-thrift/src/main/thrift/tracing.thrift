/**
 * Define thrift structs for thrift request tracing.
 */

namespace java com.twitter.finagle.thrift.thrift

/**
 * The following is from BigBrotherBird:
 *   http://j.mp/fZZnyD
 */

const string CLIENT_SEND = "cs"
const string CLIENT_RECV = "cr"
const string SERVER_SEND = "ss"
const string SERVER_RECV = "sr"

struct Endpoint {
  1: optional i32 ipv4,
  2: optional i16 port
}

struct Annotation {
  1: optional i64 timestamp,
  2: optional string value,
  3: optional Endpoint host
}

struct Span {
  1: optional i64 trace_id                  // unique trace id, use for all spans in trace
  2: optional string service_name,          // which service did this operation happen on?
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
  1: i64  trace_id,
  2: i64  span_id,
  3: optional i64 parent_span_id,
  4: bool debug
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
