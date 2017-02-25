# Redis integration with Flume NG

Implementation of Redis sources and sinks for Flume NG

[![Build Status](https://travis-ci.org/vitalibo/flume-ng-redis.svg?branch=master)](https://travis-ci.org/vitalibo/flume-ng-redis)

### Roadmap

- [x] Docker infrastructure (Redis 3 and Flume NG 1.7.0)
- [x] Sources
    - [x] List
    - [x] Subscribe
- [x] Sinks
    - [x] List
    - [x] Hash
    - [x] Set
    - [x] String
    - [x] Publish

### Build

First of all you need checkout this repository or download source codes from `master` branch.

```
git clone git@github.com:vitalibo/flume-ng-redis.git
```

The next step is build this project use following command

```
mvn clean package -Dmaven.test.skip=true
```

So, now you can run flume-ng and redis on docker infrastructure

```
docker-compose up --build
```

or, run directly flume-ng with command

```
flume-ng agent -C target/flume-ng-redis-0.1.0-SNAPSHOT.jar -n a1 -c conf -f conf/redis-list-source.conf
```

### Configuration

All redis sink and sources support next default configuration (where * dependent of configuration type).

```
agent.*.*.type = com.github.vitalibo.flume.plugin.redis.*
agent.*.*.redis.host = localhost
agent.*.*.redis.port = 6379
agent.*.*.redis.timeout = 2000
agent.*.*.redis.password = s3cr3t
```

Examples of all configurations available in folder `conf`.

#### Sources

##### List 

```
agent.sources.*.type = com.github.vitalibo.flume.plugin.redis.source.RedisListSource
agent.sources.*.redis.key = demo
```

##### Subscribe

```
agent.sources.*.type = com.github.vitalibo.flume.plugin.redis.source.RedisSubscribeSource
agent.sources.*.redis.channels = demo
```

#### Sinks

##### List

```
agent.sinks.*.type = com.github.vitalibo.flume.plugin.redis.sink.RedisListSink
agent.sinks.*.redis.key = demo
```

##### Hash

Hash sink support two strategy. In first case you need set regex pattern `redis.hash.pattern` and mapping group number onto field name. For this set `redis.hash.field.*` correct mapping (where * is number of group).

```
agent.sinks.*.type = com.github.vitalibo.flume.plugin.redis.sink.RedisHashSink
agent.sinks.*.redis.key = demo:#AUTO_INCREMENT:id
agent.sinks.*.redis.hash.pattern = (.*)\t(.*)\t(.*)\t(.*)\t(.*)
agent.sinks.*.redis.hash.field.1 = ip
agent.sinks.*.redis.hash.field.2 = date
agent.sinks.*.redis.hash.field.3 = method
agent.sinks.*.redis.hash.field.4 = host
agent.sinks.*.redis.hash.field.5 = browser
```

or, use second strategy where you need set regex pattern with named group (for example `(?<name>.*)`).

```
agent.sinks.*.type = com.github.vitalibo.flume.plugin.redis.sink.RedisHashSink
agent.sinks.*.redis.key = demo:#AUTO_INCREMENT:id
agent.sinks.*.redis.hash.pattern = (?<ip>.*)\t(?<date>.*)\t(?<method>.*)\t(?<host>.*)\t(?<browser>.*)
```

##### Set

```
agent.sinks.*.type = com.github.vitalibo.flume.plugin.redis.sink.RedisSetSink
agent.sinks.*.redis.key = demo
```

##### String

```
agent.sinks.*.type = com.github.vitalibo.flume.plugin.redis.sink.RedisStringSink
agent.sinks.*.redis.key = demo:#AUTO_INCREMENT:id
```

##### Publish

```
agent.sinks.*.type = com.github.vitalibo.flume.plugin.redis.sink.RedisPublishSink
agent.sinks.*.redis.key = redis.channels = foo, bar
```
