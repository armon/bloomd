Bloomd [![Build Status](https://travis-ci.org/armon/bloomd.png?branch=master)](https://travis-ci.org/armon/bloomd)
=========

Bloomd is a high-performance C server which is used
to expose bloom filters and operations over them to
networked clients. It uses a simple ASCII protocol
which is human readable, and similar to memcached.

[Bloom filters](http://en.wikipedia.org/wiki/Bloom_filter) are a
type of sketching or approximate data structure. They trade exactness
for efficiency of representation. Bloom filters provide a set-like
abstraction, with the important caveat that they may contain false-positives,
meaning they may claim an element is part of the set when it was
never in fact added. The rate of false positives can be tuned to meet
application demands, but reducing the error rate rapidly increases
the amount of memory required for the representation.

TL;DR: Bloom filters enable you to represent 1MM items with a false positive rate
of 0.1% in 2.4MB of RAM.


Features
--------

* Scalable non-blocking core allows for many connected
  clients and concurrent operations
* Implements scalable bloom filters, allowing dynamic filter sizes
* Supports asynchronous flushes to disk for persistence
* Supports non-disk backed bloom filters for high I/O
* Automatically faults cold filters out of memory to save resources
* Dead simple to start and administer
* FAST, FAST, FAST

Install
-------

Download and build from source:

    $ git clone https://armon@github.com/armon/bloomd.git
    $ cd bloomd
    $ pip install SCons  # Uses the Scons build system, may not be necessary
    $ scons
    $ ./bloomd

This will generate some errors related to building the test code
as it depends on libcheck. To build the test code successfully,
do the following:

    $ cd deps/check-0.9.8/
    $ ./configure
    $ make
    # make install
    # ldconfig (necessary on some Linux distros)

Then re-build bloomd. At this point, the test code should build
successfully.

For CentOS or RHEL users, the kind folks from Vortex RPM have made
a repo available with RPM's.

* Repo: http://vortex-rpm.org/
* Bloomd RPM spec: https://github.com/vortex-rpm/bloomd

Usage
-----

Bloomd can be configured using a file which is in INI format.
Here is an example configuration file:

    # Settings for bloomd
    [bloomd]
    tcp_port = 8673
    data_dir = /mnt/bloomd
    log_level = INFO
    flush_interval = 300
    workers = 2


Then run bloomd, pointing it to that file:

    bloomd -f /etc/bloomd.conf

A full list of configuration options is below.

Clients
----------

Here is a list of known client implementations:

* Python : https://github.com/kiip/bloom-python-driver
* Ruby : https://github.com/SponsorPay/bloomrb
* Erlang : https://github.com/armon/erl-bloomd
* Go : https://github.com/geetarista/go-bloomd
* Perl : https://github.com/dams/Bloomd-Client
* Node.js : https://github.com/majelbstoat/node-bloomd
* PHP: https://github.com/mdlayher/php-bloomd
* Java: https://github.com/casidiablo/java-bloomd-client

Here is a list of "best-practices" for client implementations:

* Maintain a set of open connections to the server to minimize connection time
* Make use of the bulk operations when possible, as they are more efficient.
* For long keys, it is better to do a client-side hash (SHA1 at least), and send
  the hash as the key to minimize network traffic.

Configuration Options
---------------------

Each configuration option is documented below:

 * tcp\_port : Integer, sets the tcp port to listen on. Default 8673.

 * port: Same as above. For compatibility.

 * udp\_port : Integer, sets the udp port. Currently listened on
                but otherwise unused. Default 8674.

 * bind\_address: The IP to bind to. Defaults to 0.0.0.0

 * data\_dir : The data directory that is used. Defaults to /tmp/bloomd

 * log\_level : The logging level that bloomd should use. One of:
    DEBUG, INFO, WARN, ERROR, or CRITICAL. All logs go to syslog,
    and stderr if that is a TTY. Default is DEBUG.

 * workers : This controls the number of worker threads that are used.
   Defaults to 1. If many different filters are used, it can be advantageous
   to increase this to the number of CPU cores. If only a few filters are used,
   the increased lock contention may reduce throughput, and a single worker
   may be better.

 * flush\_interval : This is the time interval in seconds in which
    filters are flushed to disk. Defaults to 60 seconds. Set to 0 to
    disable.

 * cold\_interval : If a filter is not accessed (check or set), for
    this amount of time, it is eligible to be removed from memory
    and left only on disk. If a filter is accessed, it will automatically
    be faulted back into memory. Set to 3600 seconds by default (1 hour).
    Set to 0 to disable cold faulting.

 * in\_memory : If set to 1, then all filters are in-memory ONLY by
    default. This means they are not persisted to disk, and are not
    eligible for cold fault out. Defaults to 0.

 * initial\_capacity : If a create command does not provide an initial
    capacity for a filter, this value is used. Defaults to 100K items.

 * default\_probability : If a create command does not provide a false-positive
    probability rate, this value is used. Defaults to 1/10K.

 * use\_mmap : If set to 1, the bloomd internal buffer management
    is disabled, and instead buffers use a plain mmap() and rely on
    the kernel for all management. This increases data safety in the
    case that bloomd crashes, but has adverse affects on performance
    if the total memory utilization of the system is high. In general,
    this should be left to 0, which is the default.

 * scale\_size : When a bloom filter is "scaled" up, this is the
    multiplier that is used. It should either be 2 or 4. Setting it
    to 2 will conserve memory, but is slower due to the increased number
    of filters that need to be checked. Defaults to 4.

 * probability\_reduction : This is a subtle control value that affects the
    scaling of bloom filters. It should probably not be modified. Defaults
    to 0.9.


Protocol
--------

By default, Bloomd will listen for TCP connections on port 8673.
It uses a simple ASCII protocol that is very similar to memcached.

A command has the following syntax:

    cmd [args][\r]\n

We start each line by specifying a command, providing optional arguments,
and ending the line in a newline (carriage return is optional).

There are a total of 11 commands:

* create - Create a new filter (a filter is a named bloom filter)
* list - List all filters or those matching a prefix
* drop - Drop a filters (Deletes from disk)
* close - Closes a filter (Unmaps from memory, but still accessible)
* clear - Clears a filter from the lists (Removes memory, left on disk)
* check|c - Check if a key is in a filter
* multi|m - Checks if a list of keys are in a filter
* set|s - Set an item in a filter
* bulk|b - Set many items in a filter at once
* info - Gets info about a filter
* flush - Flushes all filters or just a specified one

For the ``create`` command, the format is:

    create filter_name [capacity=initial_capacity] [prob=max_prob] [in_memory=0|1]

Note:

1. `capacity` must > 10,000 (1e4, 10K)
2. `capacity` is suggested <= 1,000,000,000 (1e9, 1G)
3. `prob` must < 0.1 (1e-1)
4. `prob` is suggested <= 0.01 (1e-2)

Where ``filter_name`` is the name of the filter,
and can contain the characters a-z, A-Z, 0-9, ., _.
If an initial capacity is provided the filter
will be created to store at least that many items in the initial filter.
Otherwise the configured default value will be used.
If a maximum false positive probability is provided,
that will be used, otherwise the configured default is used.
You can optionally specify in_memory to force the filter to not be
persisted to disk.

As an example:

    create foobar capacity=1000000 prob=0.001

This will create a filter foobar that has a 1M initial capacity,
and a 1/1000 probability of generating false positives. Valid responses
are either "Done", "Exists", or "Delete in progress". The last response
occurs if a filter of the same name was recently deleted, and bloomd
has not yet completed the delete operation. If so, a client should
retry the create in a few seconds.

The ``list`` command takes either no arguments or a set prefix, and returns information
about the matching filters. Here is an example response to a command:

    > list foo
    START
    foobar 0.001 1797211 1000000 0
    END

With the list prefix "foo", this indicates a single filter named foobar, with a probability
of 0.001 of false positives, a 1.79MB size, a current capacity of
1M items, and 0 current items. The size and capacity automatically
scale as more items are added.

The ``drop``, ``close`` and ``clear`` commands are like create, but only takes a filter name.
It can either return "Done" or "Filter does not exist". ``clear`` can also return "Filter is not proxied. Close it first.".
This means that the filter is still in-memory and not qualified for being cleared.
This can be resolved by first closing the filter.

Check and set look similar, they are either:

    [check|set] filter_name key

The command must specify a filter and a key to use.
They will either return "Yes", "No" or "Filter does not exist".


The bulk and multi commands are similar to check/set but allows for many keys
to be set or checked at once. Keys must be separated by a space:

    [multi|bulk] filter_name key1 [key_2 [key_3 [key_N]]]

The check, multi, set and bulk commands can also be called by their aliasses
c, m, s and b respectively.

The ``info`` command takes a filter name, and returns
information about the filter. Here is an example output:

    START
    capacity 1000000
    checks 0
    check_hits 0
    check_misses 0
    page_ins 0
    page_outs 0
    probability 0.001
    sets 0
    set_hits 0
    set_misses 0
    size 0
    storage 1797211
    END

The command may also return "Filter does not exist" if the filter does
not exist.

The ``flush`` command may be called without any arguments, which
causes all filters to be flushed. If a filter name is provided
then that filter will be flushed. This will either return "Done" or
"Filter does not exist".

Example
----------

Here is an example of a client flow, assuming bloomd is
running on the default port using just telnet:

    $ telnet localhost 8673
    > list
    START
    END

    > create foobar
    Done

    > check foobar zipzab
    No

    > set foobar zipzab
    Yes

    > check foobar zipzab
    Yes

    > multi foobar zipzab blah boo
    Yes No No

    > bulk foobar zipzab blah boo
    No Yes Yes

    > multi foobar zipzab blah boo
    Yes Yes Yes

    > list
    START
    foobar 0.000100 300046 100000 3
    END

    > drop foobar
    Done

    > list
    START
    END


Performance
-----------

Although extensive performance evaluations have not been done,
casual testing on a 2012 MBP with pure set/check operations
allows for a throughput of at least 600K ops/sec. On Linux,
response times can be as low as 1.5 μs.

Bloomd also supports multi-core systems for scalability, so
it is important to tune it for the given work load. The number
of worker threads can be configured either in the configuration
file, or by providing a `-w` flag. This should be set to at most
2 * CPU count. By default, only a single worker is used.

References
-----------

Here are some related works which we make use of:

* Space/Time Trade-offs in Hash Coding with Allowable Errors (Bloom): http://www.lsi.upc.edu/~diaz/p422-bloom.pdf
* Scalable Bloom Filters (Almeida et. al): http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
* Less Hashing, Same Performance: Building a Better Bloom Filter (Kirsch and Mitzenmacher): http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf

