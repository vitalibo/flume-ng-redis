package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

@AllArgsConstructor
public abstract class AbstractRedisSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractRedisSink.class);

    @Getter
    private final RedisClient client;

    public AbstractRedisSink() {
        this(new RedisClient());
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

    @Override
    public Status process() throws EventDeliveryException {
        try {
            final Channel channel = this.getChannel();

            return process(channel);
        } catch (JedisException e) {
            client.disconnect();
            throw new EventDeliveryException("Disconnected from Redis server ...", e);
        } catch (Exception e) {
            return Status.BACKOFF;
        }
    }

    private Status process(Channel channel) {
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();
            final Status status = doProcess(channel);
            transaction.commit();
            return status;
        } catch (Throwable e) {
            transaction.rollback();
            logger.error("Unexpected exception in AbstractRedisSink.", e);
            throw e;
        } finally {
            transaction.close();
        }
    }

    public abstract Status doProcess(Channel channel);

}
