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
    private Matcher key;
    @Getter
    private String counter;

    public AbstractRedisTypeSink(RedisClient client) {
        super(client);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);

        this.database = context.getInteger("redis.database", 0);
        this.counter = context.getString("redis.counter", "flume:agent:" + this.getName() + ":counter");
        String key = context.getString("redis.key");
        Preconditions.checkNotNull(key, "Redis key must be set.");
        this.key = AUTO_INCREMENT_PATTERN.matcher(key);
        Preconditions.checkState(this.key.find(), "Incorrect key format. Please use 'key:#AUTO_INCREMENT:id'.");
    }

    @Override
    public synchronized void start() {
        super.start();

        if (database != 0) {
            client.select(database);
        }
    }

    protected String nextKey() {
        Long nextId = client.incr(counter);

        return key.replaceAll(String.valueOf(nextId));
    }

}
