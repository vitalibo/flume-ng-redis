package com.github.vitalibo.flume.plugin.redis.source;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractPollableSource;

@AllArgsConstructor
public abstract class AbstractRedisSource extends AbstractPollableSource implements Configurable {

    @Getter
    protected final RedisClient client;

    public AbstractRedisSource() {
        this(new RedisClient());
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        client.configure(context);
    }

    @Override
    protected void doStart() throws FlumeException {
        client.openConnection();
    }

    @Override
    protected void doStop() throws FlumeException {
        client.disconnect();
    }

}
