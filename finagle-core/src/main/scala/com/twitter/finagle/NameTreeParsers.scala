package com.twitter.finagle

import com.twitter.io.Buf
import scala.collection.mutable.{ArrayBuffer, Buffer}

private[finagle] object NameTreeParsers {
  def parsePath(str: String): Path = new NameTreeParsers(str).parseAllPath()
  def parseNameTree(str: String): NameTree[Path] = new NameTreeParsers(str).parseAllNameTree()
  def parseDentry(str: String): Dentry = new NameTreeParsers(str).parseAllDentry()
  def parseDtab(str: String): Dtab = new NameTreeParsers(str).parseAllDtab()
}

private class NameTreeParsers private (str: String) {
  private[this] val EOI = Char.MaxValue

  private[this] val size = str.size
  private[this] var idx = 0

  private[this] def stringOfChar(char: Char) =
    if (char == EOI) "end of input"
    else "'" + char + "'"

  private[this] def illegal(expected: String, found: String): Nothing = {
    val displayStr =
      if (atEnd) s"$str[]"
      else s"${str.take(idx)}[${str(idx)}]${str.drop(idx+1)}"
    throw new IllegalArgumentException(s"$expected expected but $found found at '$displayStr'")
  }

  private[this] def illegal(expected: Char, found: String): Nothing =
    illegal(stringOfChar(expected), found)

  private[this] def illegal(expected: String, found: Char): Nothing =
    illegal(expected, stringOfChar(found))

  private[this] def illegal(expected: Char, found: Char): Nothing =
    illegal(stringOfChar(expected), stringOfChar(found))

  private[this] def peek: Char =
    if (atEnd) EOI
    else str(idx)

  private[this] def next() { idx += 1 }

  private[this] def maybeEat(char: Char) =
    if (peek != char) false
    else {
      next()
      true
    }

  private[this] def eat(char: Char) {
    if (!maybeEat(char)) illegal(char, peek)
  }

  private[this] def eatWhitespace() {
    while (!atEnd && str(idx).isWhitespace)
      next()
  }

  private[this] def atEnd() = idx >= size

  private[this] def ensureEnd() {
    if (!atEnd)
      illegal(EOI, peek)
  }

  private[this] def parseHexChar(): Char =
    peek match {
      case c@('0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'
         |'A'|'B'|'C'|'D'|'E'|'F'|'a'|'b'|'c'|'d'|'e'|'f') =>
        next()
        c

      case c => illegal("hex char", c)
    }

  private[this] def isLabelChar(c: Char) = Path.isShowable(c) || c == '\\'

  // extract the underlying buf to avoid a copy in toByteArray
  private[this] class Baos(size: Int) extends java.io.ByteArrayOutputStream(size) {
    def getBuf() = buf
  }

  private[this] def parseLabel(): Buf = {
    val baos = new Baos(16)

    do {
      peek match {
        case c if Path.isShowable(c) =>
          next()
          baos.write(c.toByte)

        case '\\' =>
          next()
          eat('x')
          val fst = parseHexChar()
          val snd = parseHexChar()
          baos.write(Character.digit(fst, 16) << 4 | Character.digit(snd, 16))

        case c => illegal("label char", c)
      }
    } while (isLabelChar(peek))

    Buf.ByteArray.Owned(baos.getBuf, 0, baos.size)
  }

  private[this] def isNumberChar(c: Char) = c.isDigit || c == '.'

  private[this] def parseNumber(): Double = {
    val sb = new StringBuilder
    var seenDot = false

    while (isNumberChar(peek)) {
      if (peek == '.') {
        if (seenDot) illegal("number char", peek)
        else seenDot = true
      }
      sb += peek
      next()
    }
    if (sb.length == 1 && sb.charAt(0) == '.') {
      illegal("weight", '.')
    }
    sb.toString.toDouble // can fail if string is too long
  }

  private[this] def parsePath(): Path = {
    eatWhitespace()
    eat('/')

    if (!isLabelChar(peek))
      Path.empty

    else {
      val labels = Buffer[Buf]()

      do {
        labels += parseLabel()
      } while (maybeEat('/'))

      Path(labels:_*)
    }
  }

  private[this] def parseTree(): NameTree[Path] = {
    val trees = Buffer[NameTree[Path]]()

    do {
      trees += parseTree1()
      eatWhitespace()
    } while (maybeEat('|'))

    if (trees.size > 1)
      NameTree.Alt(trees:_*)
    else
      trees(0)
  }

  private[this] def parseTree1(): NameTree[Path] = {
    val trees = Buffer[NameTree.Weighted[Path]]()

    do {
      trees += parseWeighted()
      eatWhitespace()
    } while (maybeEat('&'))

    if (trees.size > 1)
      NameTree.Union(trees:_*)
    else
      trees(0).tree
  }

  private[this] def parseSimple(): NameTree[Path] = {
    eatWhitespace()
    peek match {

      case '(' =>
        next()
        val tree = parseTree()
        eatWhitespace()
        eat(')')
        tree

      case '/' =>
        NameTree.Leaf(parsePath())

      case '!' =>
        next()
        NameTree.Fail

      case '~' =>
        next()
        NameTree.Neg

      case '$' =>
        next()
        NameTree.Empty

      case c =>
        illegal("simple", c)
    }
  }

  private[this] def parseWeighted(): NameTree.Weighted[Path] = {
    eatWhitespace()
    val weight =
      if (!isNumberChar(peek)) NameTree.Weighted.defaultWeight
      else {
        val weight = parseNumber()
        eatWhitespace()
        eat('*')
        eatWhitespace()
        weight
      }
    NameTree.Weighted(weight, parseSimple())
  }

  private[this] def parseDentry(): Dentry = {
    val path = parsePath()
    eatWhitespace()
    eat('=')
    eat('>')
    val tree = parseTree()
    Dentry(path, tree)
  }

  private[this] def parseDtab(): Dtab = {
    val dentries = ArrayBuffer[Dentry]()

    do {
      eatWhitespace()
      if (!atEnd) {
        dentries += parseDentry()
        eatWhitespace()
      }
    } while (maybeEat(';'))

    Dtab(dentries)
  }

  def parseAllPath(): Path = {
    val path = parsePath()
    eatWhitespace()
    ensureEnd()
    path
  }

  def parseAllNameTree(): NameTree[Path] = {
    val tree = parseTree()
    eatWhitespace()
    ensureEnd()
    tree
  }

  def parseAllDentry(): Dentry = {
    val dentry = parseDentry()
    eatWhitespace()
    ensureEnd()
    dentry
  }

  def parseAllDtab(): Dtab = {
    val dtab = parseDtab()
    eatWhitespace()
    ensureEnd()
    dtab
  }
}
