package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class AbstractRedisSink extends AbstractSink implements Configurable {

    private final RedisClient client;

    public AbstractRedisSink() {
        this(new RedisClient());
    }

    public AbstractRedisSink(RedisClient client) {
        this.client = client;
    }

    @Override
    public void configure(Context context) {
        client.configure(context);
    }

    @Override
    public synchronized void start() {
        super.start();

        client._connect();
    }

    @Override
    public synchronized void stop() {
        client.disconnect();

        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

}
