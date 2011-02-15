/**
 * Define thrift structs for thrift request tracing.
 */

namespace java com.twitter.finagle.thrift

struct TranscriptRecord {
  1: i32     host;
  2: string  vm_id;
  3: i64     span_id;
  4: i64     parent_span_id;
  5: i64     timestamp_ms;
  6: string  message;
}

/**
 * TracedRequest defines trace headers. These carry the span data, and
 * a flag indicating whether the request is to be debugged.
 */
struct TracedRequest {
  1: i64   parent_span_id;
  2: bool  debug;
}

/**
 * The TracedResponse carries a reply header for tracing. These are
 * empty unless the request is being debugged, in which case a
 * transcript is copied.
 */
struct TracedResponse {
  1: list<TranscriptRecord> transcript;
}

/**
 * These are connection-level trace options negotiated during protocol
 * upgrade. (Intentionally left blank: for future use).
 */
struct TraceOptions {}
