package com.twitter.finagle.http

import com.twitter.io.Buf

/*
 * HTML form element.
 */
sealed abstract class FormElement

/*
 * HTML form simple input field.
 */
final case class SimpleElement(name: String, content: String) extends FormElement

/*
 * HTML form file input field.
 */
final case class FileElement(
  name: String,
  content: Buf,
  contentType: Option[String] = None,
  filename: Option[String] = None,
  isText: Boolean = false)
    extends FormElement
