# Embedding Maxwell
***
Maxwell typically runs as a command-line program. However, for advanced uses it
is possible to run maxwell from any JVM-based language.

Some fairly incomplete API documentation is available here:<br/>
[https://maxwells-daemon.io/apidocs](https://maxwells-daemon.io/apidocs)

# Compatibility caveat
***
Maxwell makes every attempt to remain backwards compatible. However this
only applies to the command-line usage - Maxwell's Java API may change without
notice.

However (and unless otherwise indicated) breaking API changes will result in
a type error - i.e. if your code still compiles, then the API has not changed.
