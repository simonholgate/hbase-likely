# hbase-likely

A collection of functions to explore Twitter data stored in HBase for a 
likely.co project.

## Usage

With the HBase tables of 'tweets', and 'short_urls' the 'language' and 'urls'
tables must be created in the HBase shell with:
create 'urls', 'url'
create 'language', 'lang'
Then run '-main' in the repl or lein run.

Cascalog queries are defined in the 'hbase-likely.query' namespace.

## License

Copyright © 2012 Simon Holgate

Distributed under the Eclipse Public License, the same as Clojure.
