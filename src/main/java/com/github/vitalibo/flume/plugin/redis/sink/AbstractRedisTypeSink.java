package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flume.Context;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@NoArgsConstructor
public abstract class AbstractRedisTypeSink extends AbstractRedisSink {

    private static final Pattern AUTO_INCREMENT_PATTERN = Pattern.compile("#AUTO_INCREMENT");

    @Getter
    private Integer database;
    @Getter
    private String key;
    @Getter
    private String counter;

    private Matcher keyMatcher;

    AbstractRedisTypeSink(RedisClient client) {
        super(client);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);

        this.database = context.getInteger("redis.database", 0);
        this.counter = context.getString("redis.counter", "flume:agent:" + this.getName() + ":counter");
        this.key = context.getString("redis.key");
        Preconditions.checkNotNull(key, "Redis key must be set.");
        Matcher matcher = AUTO_INCREMENT_PATTERN.matcher(key);
        if (matcher.find()) {
            this.keyMatcher = matcher;
        }
    }

    @Override
    public synchronized void start() {
        super.start();

        if (database != 0) {
            client.select(database);
        }
    }

    String nextKey() {
        if (keyMatcher == null) {
            throw new IllegalArgumentException("Incorrect key format. Please use 'key:#AUTO_INCREMENT:id'.");
        }

        Long nextId = client.incr(counter);
        return keyMatcher.replaceAll(String.valueOf(nextId));
    }

}
