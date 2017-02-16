package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import lombok.NoArgsConstructor;
import org.apache.flume.Channel;
import org.apache.flume.Event;

@NoArgsConstructor
public class RedisStringSink extends AbstractRedisTypeSink {

    RedisStringSink(RedisClient client) {
        super(client);
    }

    @Override
    public Status doProcess(Channel channel) {
        Event event = channel.take();

        if (event == null) {
            return Status.BACKOFF;
        }

        client.set(nextKey(), new String(event.getBody()));
        return Status.READY;
    }

}
