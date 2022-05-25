## rustCacheDb

The Rust Caching Data Base is a minimalistic key/ val database with a focus on fast temporary data storage. The Rust lib can be implemented in every project easily and does not require deamons or instances except for an additional thread. The lib does not depend on any dependencies except for the Rust std lib and is highly portable. All data is stored in memory(heap).
The goal was to find a compromise between (read/ write) speed, safety, portability as an alternative to existing db solutions.

## Optimization

### Only send pull request if not already in request (by an other actor)

In order to achiev fast but also up to date pulls, the Cache Client keeps a record of all the pulled data. If a certain key is already in the process of being pulled, instead of sending another pull request, the Cache Client waits for the result of the already sent pull request. This may sound trivial but is difficult to implement in an effective and potent manner. If the request finally arrives, a new request has to be sent to the server instance.

This concept guarantees that the data is as up to date as possible(if speed is of priority) whilest still not being "old". In a worst case, two requests are made in which the first actually sends his reply and the second waits on the first reply(instead of sending his own). The data did change after the arrival(and sent out reply) of the first request though which means that potential critical data updates were lost. On the plus side, a ton of time was saved.

### Optimization Performance

#### Timing

I've written a very similiar (functionally and architecture wise) project in C (can be found [here](https://github.com/luickk/tempCacheDb/blob/main/src/tempCacheDb.c) wihtout the pull request optimization, which takes 55 μs(micross.) per pull request. 

This projects achieves 15 μs through this optimisation.

#### Numbers

- The `pull_async` function "skips" (instead waits for the pull reply already sent) *402/500* requests. That means that instead of 500 requests, only 100 were made. That's 90% less.
- If the data is pulled synchronously from two thread 498/500 are skipped. So instead of 500, only two requests were actually made. 

### Cost

Regular cost flow: 

`request send(buffer assemble; tcp write) -> cache client handler waits & parses incoming data (actual parse; linear search by key to write to correct requesting client/server)` 

Optimized cost flow: 

`request send(linear seach to look wether a request has already been made; request has not been made already(buffer assemble; tcp write); waiting for pulling condvar(is set by cache client handler) to turn false) -> cache client handler waits & parses incoming data (actual parse; linear search by key to write to correct "requestor"; setting condvar)` 

## Features

- local key/val database
- remote push/pull of data
- full concurrency support

## Tcp protocol

`uint8_t opCode(pull=1, push=2, pullReply=3) - uint16_t (query)keySize - char[] (query)key - uint16_t(val) valSize - char[] val`