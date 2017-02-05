package com.github.vitalibo.flume.plugin.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

@NoArgsConstructor
@AllArgsConstructor
public class RedisClient implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    @Delegate
    private Jedis client;
    @Getter
    private String host;
    @Getter
    private int port;
    @Getter
    private int timeout;
    @Getter
    private String password;

    @Override
    public void configure(Context context) {
        this.host = context.getString("redis.host", "localhost");
        this.port = context.getInteger("redis.port", 6379);
        this.timeout = context.getInteger("redis.timeout", 2000);
        this.password = context.getString("redis.password", null);
        this.client = new Jedis(host, port, timeout);
    }

    @SneakyThrows(InterruptedException.class)
    public void openConnection() {
        logger.info("Redis connecting...");
        for (int retry = 0; true; retry++) {
            try {
                if (password != null) {
                    client.auth(password);
                }

                client.connect();
                if (!"PONG".equals(client.ping())) {
                    throw new JedisConnectionException("Server returned values not equals 'PONG'.");
                }

                logger.info("Redis connected to " + host + ":" + port);
                return;
            } catch (JedisException e) {
                logger.error("Connection failed.", e);
                if (retry >= 2) {
                    throw e;
                }

                logger.info("Waiting for {} milliseconds...", timeout);
                Thread.sleep(timeout);
            }
        }
    }

}
