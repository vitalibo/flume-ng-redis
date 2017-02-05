package com.github.vitalibo.flume.plugin.redis;

import org.apache.flume.Context;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisClientTest {

    @Mock
    private Jedis mockJedis;

    private RedisClient client;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        client = new RedisClient(mockJedis, "localhost", 6379, 100, "qwerty12345");
    }

    @Test
    public void testDefaultConfiguration() {
        client = new RedisClient();

        client.configure(new Context());

        Assert.assertEquals(client.getHost(), "localhost");
        Assert.assertEquals(client.getPort(), 6379);
        Assert.assertEquals(client.getTimeout(), 2000);
        Assert.assertNull(client.getPassword());
    }

    @Test
    public void testConfiguration() {
        client = new RedisClient();
        Context context = new Context(Stream
            .of(
                entry("host", "192.168.1.1"), entry("port", "3000"),
                entry("timeout", "1000"), entry("password", "qwerty12345"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        client.configure(context);

        Assert.assertEquals(client.getHost(), "192.168.1.1");
        Assert.assertEquals(client.getPort(), 3000);
        Assert.assertEquals(client.getTimeout(), 1000);
        Assert.assertEquals(client.getPassword(), "qwerty12345");
    }

    @Test
    public void testConnect() {
        Mockito.when(mockJedis.ping()).thenReturn("PONG");

        client.openConnection();

        Assert.assertTrue(true);
    }

    @Test
    public void testConnectWithRetry() {
        Mockito.when(mockJedis.ping()).thenReturn("NOT PONG").thenReturn("PONG");

        client.openConnection();

        Assert.assertTrue(true);
    }

    @Test(expectedExceptions = JedisConnectionException.class)
    public void testFailConnect() {
        Mockito.doThrow(JedisConnectionException.class).when(mockJedis).connect();

        client.openConnection();

        Assert.assertTrue(false);
    }

    @Test(expectedExceptions = JedisConnectionException.class)
    public void testFailConnectExpectedPong() {
        Mockito.when(mockJedis.ping()).thenReturn("NOT PONG");

        client.openConnection();

        Assert.assertTrue(false);
    }

    @Test(expectedExceptions = JedisDataException.class)
    public void testFailAuthorization() {
        Mockito.when(mockJedis.auth(Mockito.anyString())).thenThrow(JedisDataException.class);

        client.openConnection();

        Assert.assertTrue(false);
    }

    private static Map.Entry<String, String> entry(String key, String value) {
        return new AbstractMap.SimpleEntry<>("redis." + key, value);
    }

}
