# Finagle

[![Build status](https://travis-ci.org/twitter/finagle.svg?branch=develop)](https://travis-ci.org/twitter/finagle)
[![Coverage status](https://img.shields.io/coveralls/twitter/finagle/develop.svg)](https://coveralls.io/r/twitter/finagle?branch=develop)
[![Project status](https://img.shields.io/badge/status-active-brightgreen.svg)](#status)
[![Gitter](https://img.shields.io/badge/gitter-join%20chat-green.svg)](https://gitter.im/twitter/finagle?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://img.shields.io/maven-central/v/com.twitter/finagle_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.twitter/finagle_2.11)

## Status

This project is used in production at Twitter (and many other organizations),
and is being actively developed and maintained.

![Finagle logo](https://raw.github.com/twitter/finagle/master/doc/src/sphinx/_static/logo_small.png)

## Getting involved

* Website: https://twitter.github.io/finagle/
* Source: https://github.com/twitter/finagle/
* Mailing List: [finaglers@googlegroups.com](https://groups.google.com/forum/#!forum/finaglers)
* IRC: `#finagle` on Freenode

Finagle is an extensible RPC system for the JVM, used to construct
high-concurrency servers. Finagle implements uniform client and server APIs for
several protocols, and is designed for high performance and concurrency. Most of
Finagle’s code is protocol agnostic, simplifying the implementation of new
protocols.

For extensive documentation, please see the
[user guide](https://twitter.github.io/finagle/guide/) and
[API documentation](https://twitter.github.io/finagle/docs/#com.twitter.finagle.package)
websites. Documentation improvements are always welcome, so please send patches
our way.

## Adopters

The following are a few of the companies that are using Finagle:

* [Foursquare](https://foursquare.com/)
* [ING Bank](https://ing.nl)
* [Pinterest](https://www.pinterest.com/)
* [SoundCloud](https://soundcloud.com/)
* [Tumblr](https://www.tumblr.com/)
* [Twitter](https://twitter.com/)

For a more complete list, please see
[our adopter page](https://github.com/twitter/finagle/blob/master/ADOPTERS.md).
If your organization is using Finagle, consider adding a link there and sending
us a pull request!

## Contributing

The `master` branch of this repository contains the latest stable release of
Finagle, and weekly snapshots are published to the `develop` branch. In general
pull requests should be submitted against `develop`. See
[CONTRIBUTING.md](https://github.com/twitter/finagle/blob/master/CONTRIBUTING.md)
for more details about how to contribute.
