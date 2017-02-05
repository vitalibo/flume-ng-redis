package com.github.vitalibo.flume.plugin.redis.source;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

public class AbstractRedisSource extends AbstractSource implements Configurable {

    private final RedisClient client;

    public AbstractRedisSource() {
        this(new RedisClient());
    }

    public AbstractRedisSource(RedisClient client) {
        this.client = client;
    }

    @Override
    public void configure(Context context) {
        client.configure(context);
    }

    @Override
    public synchronized void start() {
        super.start();

        client.openConnection();
    }

    @Override
    public synchronized void stop() {
        client.disconnect();

        super.stop();
    }

}
