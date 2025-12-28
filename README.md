# Toy Redis in Go

This is my take on CodeCrafters' ["Build Your Own Redis" Challenge](https://app.codecrafters.io/r/glorious-mallard-480161). Currently 18 / 97 stages complete, done with "basic" and "lists" stages.

> In this challenge, you'll build a toy Redis clone that's capable of handling basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about event loops, the Redis protocol and more.

## Functionalities

- Commands: `PING`, `ECHO`, `SET`, `GET`, `LRANGE`, `LLEN`, `RPUSH`, `LPUSH`, `LPOP`, `BLPOP`;
- Accepts multiple concurrent connections;
- `SET` supports expiry;
- Parses and writes Redis protocol directly;
- `BLPOP` supports timeout;

### TODO

- Streams;
- `TYPE` command;
- Transactions: `MULTI`, `EXEC`, `DISCARD` and `INCR` commands;
- Replication;
- RDB persistence;
- Pub/sub;
- Sorted sets;
- Geospatial commands;
- Authentication;

## Highlights

- Redis has a single writer thread. So does my implementation (for now, there's a goroutine for each value type (strings / lists));
- Easily scalable by sharding concurrent hashmaps into buckets with their own locks and writer goroutines. This would bite me in the ass later for multi-key operations, though;
- Parsing and writing Redis protocol;
- Go does not have a double-ended-queue standard implementation (although there's a doubly linked list), so I made my own generic infinite growable circular buffer with cool bit hacks;
- Priority queues and timers to handle expiry efficiently and deterministcally (instead of tombstone accumulation or sampling);

**Note**: Head over to [codecrafters.io](https://app.codecrafters.io/r/glorious-mallard-480161) to try the challenge.
