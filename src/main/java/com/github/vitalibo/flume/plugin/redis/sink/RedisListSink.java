package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import lombok.NoArgsConstructor;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

@NoArgsConstructor
public class RedisListSink extends AbstractRedisTypeSink {

    RedisListSink(RedisClient client) {
        super(client);
    }

    @Override
    public Status doProcess(Channel channel) throws EventDeliveryException {
        Event event = channel.take();

        if (event == null) {
            return Status.BACKOFF;
        }

        long n = client.rpush(getKey(), new String(event.getBody()));
        if (n <= 0) {
            throw new EventDeliveryException("Can't push even into list.");
        }

        return Status.READY;
    }

}
