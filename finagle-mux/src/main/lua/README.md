Muxshark
========

A wireshark plugin for the Mux protocol.

Features
--------

This wireshark dissector provides basic decoding of all
Mux messages including:

* Frame length
* Message type
* Tag number

`Tdispatch` messages also include decoding of contexts, destination
and Dtabs.

Installation
------------

Wireshark is available at [wireshark.org](https://www.wireshark.org/).

Copy `mux_dissector.lua` to your wireshark
[personal plugins directory](https://www.wireshark.org/docs/wsug_html_chunked/ChAppFilesConfigurationSection.html).
On Mac OS X, this is currently `$HOME/.wireshark/plugins` and you should
create this directory if it does not yet exist.

The dissector will be available after the next launch of the application.

Usage
-----

Launch Wireshark or tshark and open it with a tcpdump capture that includes
Mux packets. The heuristic detector should recognize mux packets, but should
it fail, use "Decode As..." on a packet.