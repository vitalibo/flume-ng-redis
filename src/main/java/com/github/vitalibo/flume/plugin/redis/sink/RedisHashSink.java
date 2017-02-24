package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import com.github.vitalibo.flume.plugin.redis.sink.handler.HashesTranslator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@NoArgsConstructor
public class RedisHashSink extends AbstractRedisTypeSink {

    private static final Logger logger = LoggerFactory.getLogger(RedisHashSink.class);

    @Getter
    private HashesTranslator translator;

    RedisHashSink(RedisClient client) {
        super(client);
    }

    @Override
    public void configure(Context context) {
        this.configure(context, HashesTranslator.from(context));
    }

    void configure(Context context, HashesTranslator translator) {
        super.configure(context);

        this.translator = translator;
    }

    @Override
    public Status doProcess(Channel channel) throws EventDeliveryException {
        Event event = channel.take();

        if (event == null) {
            return Status.BACKOFF;
        }

        try {
            Map<String, String> hashes = translator.apply(new String(event.getBody()));

            client.hmset(nextKey(), hashes);
        } catch (IllegalStateException e) {
            logger.warn("Skip event with incorrect format.", e);
            return Status.BACKOFF;
        }

        return Status.READY;
    }

}
