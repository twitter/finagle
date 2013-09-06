package com.twitter.finagle.http.filter

/** Adds headers to support Cross-origin resource sharing */
object CorsFilter {
  def apply(origin:  String = "*",
            methods: String = "GET",
            headers: String = "x-requested-with",
            exposes: String = "") = new AddResponseHeadersFilter(Map(
    "Access-Control-Allow-Origin"  -> origin,
    "Access-Control-Allow-Methods" -> methods,
    "Access-Control-Allow-Headers" -> headers,
    "Access-Control-Expose-Headers" -> exposes))
}
