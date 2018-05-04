<div align="center">
  <img src="https://github.com/twitter/finagle/blob/develop/doc/src/sphinx/_static/logo_medium.png"><br><br>
</div>


# Finagle
This project is used in production at Twitter (and many other organizations),and is being actively developed and maintained.

[![Build status](https://travis-ci.org/twitter/finagle.svg?branch=develop)](https://travis-ci.org/twitter/finagle)
[![Codecov](https://codecov.io/gh/twitter/finagle/branch/develop/graph/badge.svg)](https://codecov.io/gh/twitter/finagle)
[![Project status](https://img.shields.io/badge/status-active-brightgreen.svg)](#status)
[![Gitter](https://badges.gitter.im/twitter/finagle.svg)](https://gitter.im/twitter/finagle?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.twitter/finagle-core_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.twitter/finagle-core_2.12)



## Releases

[Releases](https://maven-badges.herokuapp.com/maven-central/com.twitter/finagle_2.12)
are done on an approximately monthly schedule. While [semver](http://semver.org/)
is not followed, the [changelogs](CHANGES) are detailed and include sections on
public API breaks and changes in runtime behavior.

## Getting involved

* Website: https://twitter.github.io/finagle/
* Source: https://github.com/twitter/finagle/
* Mailing List: [finaglers@googlegroups.com](https://groups.google.com/forum/#!forum/finaglers)
* Chat: https://gitter.im/twitter/finagle
* Blog: https://finagle.github.io/blog/

Finagle is an extensible RPC system for the JVM, used to construct
high-concurrency servers. Finagle implements uniform client and server APIs for
several protocols, and is designed for high performance and concurrency. Most of
Finagle’s code is protocol agnostic, simplifying the implementation of new
protocols.

For extensive documentation, please see the
[user guide](https://twitter.github.io/finagle/guide/) and
[API documentation](https://twitter.github.io/finagle/docs/com/twitter/finagle)
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

We feel that a welcoming community is important and we ask that you follow Twitter's
[Open Source Code of Conduct](https://github.com/twitter/code-of-conduct/blob/master/code-of-conduct.md)
in all interactions with the community.

The `master` branch of this repository contains the latest stable release of
Finagle, and weekly snapshots are published to the `develop` branch. In general
pull requests should be submitted against `develop`. See
[CONTRIBUTING.md](https://github.com/twitter/finagle/blob/master/CONTRIBUTING.md)
for more details about how to contribute.
