# Finagle User Guide

To generate the user guide website locally, `cd` into the `finagle`
directory and run the following commands.

    $ ./sbt finagle-doc/make-site
    $ open doc/target/site/index.html

To publish a new version of the user guide to GitHub Pages, execute
the script `finagle/pushsite.bash`.
