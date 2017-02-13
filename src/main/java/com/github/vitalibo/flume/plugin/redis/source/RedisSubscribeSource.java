package com.github.vitalibo.flume.plugin.redis.source;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisSubscribeSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSubscribeSource.class);

    @Getter
    private final RedisClient client;
    @Getter
    @Setter(AccessLevel.PACKAGE)
    private RedisSubscriber subscriber;
    @Getter
    private String[] channels;

    public RedisSubscribeSource() {
        this(new RedisClient());
        this.subscriber = new RedisSubscriber();
    }

    RedisSubscribeSource(RedisClient client) {
        this.client = client;
    }

    @Override
    public void configure(Context context) {
        client.configure(context);

        String channels = context.getString("redis.channels");
        Preconditions.checkNotNull(channels, "Redis subscribe channel must be set.");
        this.channels = channels.split(",");
    }

    @Override
    public synchronized void start() {
        super.start();

        Thread thread = new Thread(subscriber);
        thread.start();
    }

    @Override
    public synchronized void stop() {
        subscriber.unsubscribe();

        super.stop();
    }

    class RedisSubscriber extends JedisPubSub implements Runnable {

        private boolean interrupted;

        @Override
        public void run() {
            while (!interrupted) {
                try {
                    client.subscribe(this, channels);
                } catch (JedisConnectionException e) {
                    LOGGER.error("Disconnected from Redis server ...", e);
                    client.connect();
                } catch (Exception e) {
                    LOGGER.error("Unexpected exception in Redis RedisSubscriber.", e);
                    throw e;
                }
            }
        }

        @Override
        public void onMessage(String channel, String message) {
            ChannelProcessor processor = getChannelProcessor();
            Event event = EventBuilder.withBody(message.getBytes());
            processor.processEvent(event);
        }

        @Override
        public void unsubscribe() {
            interrupt();

            super.unsubscribe();
        }

        void interrupt() {
            interrupted = true;
        }

    }

}
