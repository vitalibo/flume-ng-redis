package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.stream.Stream;

public class AbstractRedisTypeSinkTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private Channel mockChannel;

    private AbstractRedisTypeSink sink;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        sink = new EmbeddedRedisSink(mockClient);
        sink.setChannel(mockChannel);
    }

    @Test
    public void testConfigure() {
        Context context = makeContext("demo:#AUTO_INCREMENT");

        sink.configure(context);

        Mockito.verify(mockClient).configure(context);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testIncorrectKeyFormat() {
        sink.configure(makeContext("incorrect format"));

        sink.nextKey();

        Assert.assertTrue(false);
    }

    @Test
    public void testChooseDatabase() {
        sink.configure(makeContext("demo:#AUTO_INCREMENT"));

        sink.start();

        Mockito.verify(mockClient).select(5);
    }

    @Test
    public void testNextKey() {
        Context context = makeContext("demo:#AUTO_INCREMENT");
        Mockito.when(mockClient.incr(Mockito.anyString())).thenReturn(1L).thenReturn(2L).thenReturn(3L);
        sink.configure(context);

        String[] keys = Stream.generate(() -> sink.nextKey())
            .limit(4)
            .toArray(String[]::new);

        Assert.assertEquals(keys, new String[]{"demo:1", "demo:2", "demo:3", "demo:3"});
    }

    private static Context makeContext(String key) {
        Context context = new Context();
        context.put("redis.key", key);
        context.put("redis.database", "5");
        return context;
    }

    private static class EmbeddedRedisSink extends AbstractRedisTypeSink {

        EmbeddedRedisSink(RedisClient client) {
            super(client);
        }

        @Override
        public Status doProcess(Channel channel) {
            return Status.READY;
        }

    }

}
