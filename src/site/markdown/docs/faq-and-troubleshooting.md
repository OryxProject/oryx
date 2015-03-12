## Tests show errors, but still pass?

[This issue](https://github.com/OryxProject/oryx/issues/73) catalogs the problem. These seem
to be ignorable errors that are attributable to the fact that a Zookeeper and Kafka process
are started and stopped rapidly. Although this is done cleanly with some built-in waiting, it
does not seem sufficient.

There is likely a better answer but exceptions shown in the issue above can be ignored.
