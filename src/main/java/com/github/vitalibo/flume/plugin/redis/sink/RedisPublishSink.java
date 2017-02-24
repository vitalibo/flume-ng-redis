package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import java.util.Arrays;
import java.util.List;

@NoArgsConstructor
public class RedisPublishSink extends AbstractRedisSink {

    @Getter
    private List<String> channels;

    RedisPublishSink(RedisClient client) {
        super(client);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);

        String channels = context.getString("redis.channels");
        Preconditions.checkNotNull(channels, "Redis channels must be set.");
        this.channels = Arrays.asList(channels.split(","));
    }

    @Override
    public Status doProcess(Channel channel) throws EventDeliveryException {
        Event event = channel.take();

        if (event == null) {
            return Status.BACKOFF;
        }

        long n = channels.stream()
            .mapToLong(c -> client.publish(c.trim(), new String(event.getBody())))
            .sum();
        if (n <= 0) {
            throw new EventDeliveryException("Event published, but not received.");
        }

        return Status.READY;
    }

}
