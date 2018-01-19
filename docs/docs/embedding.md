### Embedding Maxwell
***
Maxwell typically runs as a command-line program. However, for advanced use it
is possible to run maxwell from any JVM-based language. Currently the source of
truth is the source code (there is no published API documentation). Pull requests
to better document embedded Maxwell uses are welcome.

### Compatibility caveat
***
Maxwell makes every attempt to remain backwards compatible. However this
only applies to the command-line usage - Maxwell's Java API may change without
notice.

However (and unless otherwise indicated) breaking API changes will result in
a type error - i.e. if your code still compiles, then the API has not changed.
