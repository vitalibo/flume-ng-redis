package com.github.vitalibo.flume.plugin.redis;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);

    @Delegate
    private Jedis client;
    @Getter
    private String host;
    @Getter
    private int port;
    @Getter
    private int timeout;

    private String password;

    public RedisClient() {
        super();
    }

    public RedisClient(Jedis client, String host, int port, int timeout, String password) {
        this.client = client;
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.password = password;
    }

    @Override
    public void configure(Context context) {
        this.host = context.getString("redis.host", "localhost");
        this.port = context.getInteger("redis.port", 6379);
        this.timeout = context.getInteger("redis.timeout", 2000);
        this.password = context.getString("redis.password", null);
    }

    @SneakyThrows(InterruptedException.class)
    public boolean _connect() {
        LOGGER.info("Redis connecting...");
        for (int i = 0; i < 10; i++) {
            try {
                client = new Jedis(host, port, timeout);

                if (password != null) {
                    client.auth(password);
                }

                client.connect();
                LOGGER.info("Redis Connected to " + host + ":" + port);
                return true;
            } catch (JedisConnectionException e) {
                LOGGER.error("Connection failed.", e);
                LOGGER.info("Waiting for 10 seconds...");
                Thread.sleep(10000);
            }
        }

        return false;
    }

}
