package com.github.vitalibo.flume.plugin.redis.source;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

@NoArgsConstructor
public class RedisListSource extends AbstractRedisSource implements PollableSource {

    private static final Logger logger = LoggerFactory.getLogger(RedisListSource.class);

    @Getter
    private Integer database;
    @Getter
    private String key;

    public RedisListSource(RedisClient client, Integer database, String key) {
        super(client);
        this.database = database;
        this.key = key;
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        super.doConfigure(context);

        this.database = context.getInteger("redis.database", 0);
        this.key = context.getString("redis.key");
        Preconditions.checkNotNull(key, "Redis key must be set.");
    }

    @Override
    protected void doStart() throws FlumeException {
        super.doStart();

        if (database != 0) {
            client.select(database);
        }
    }

    @Override
    protected Status doProcess() throws EventDeliveryException {
        try {
            String message = client.rpop(key);
            if (message == null) {
                return Status.BACKOFF;
            }

            try {
                Event event = EventBuilder.withBody(message.getBytes());
                ChannelProcessor channel = this.getChannelProcessor();
                channel.processEvent(event);
            } catch (Exception e) {
                logger.error("Can't process event.", e);
                client.rpush(key, message);
                throw e;
            }

            return Status.READY;
        } catch (JedisException e) {
            logger.error(e.getMessage(), e);
            client.disconnect();
            return Status.BACKOFF;
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            throw new EventDeliveryException(e);
        }
    }

}
