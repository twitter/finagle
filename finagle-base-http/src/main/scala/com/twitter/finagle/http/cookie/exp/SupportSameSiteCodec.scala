package com.twitter.finagle.http.cookie.exp

import com.twitter.app.GlobalFlag

/**
 * Enables / disables SameSite support in the CookieCodec.
 */
object supportSameSiteCodec extends GlobalFlag[Boolean](
  false, // disabled by default
  "Allow the SameSite attribute to be added to the Set-Cookie header on Responses"
)
