# Hermann

A Ruby gem implementing a minimal Kafka Producer as a wrapper around the
excellent C library [librdkafka](https://github.com/edenhill/librdkafka).

### Usage

- Kafka 0.8 is supported.
- Ruby 2.1.3 is tested, but anything 1.9.3+ probably works.

```ruby
require 'hermann/producer'

p = Hermann::Producer.new('topic', ['localhost:6667'])  # arguments: topic, list of brokers
f = p.push('hello world')
f.state
p.tick_reactor
f.state
```

