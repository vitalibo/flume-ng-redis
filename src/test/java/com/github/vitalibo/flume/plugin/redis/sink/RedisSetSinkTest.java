package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RedisSetSinkTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private Channel mockChannel;
    @Mock
    private Event mockEvent;

    private RedisSetSink sink;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        sink = new RedisSetSink(mockClient);
        sink.configure(makeContext());
    }

    @Test
    public void testProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockClient.sadd(Mockito.anyString(), Mockito.anyString())).thenReturn(1L);
        Mockito.when(mockEvent.getBody()).thenReturn("test data".getBytes());

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.READY);
        Mockito.verify(mockClient).sadd("test.key", "test data");
    }

    @Test
    public void testBackoffProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(null);

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.BACKOFF);
        Mockito.verify(mockClient, Mockito.never()).sadd(Mockito.anyString(), Mockito.anyString());
    }

    @Test(expectedExceptions = EventDeliveryException.class)
    public void testFailProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockClient.sadd(Mockito.anyString(), Mockito.anyString())).thenReturn(0L);
        Mockito.when(mockEvent.getBody()).thenReturn("test data".getBytes());

        sink.doProcess(mockChannel);

        Assert.assertTrue(false);
    }

    private static Context makeContext() {
        Context context = new Context();
        context.put("redis.key", "test.key");
        return context;
    }

}
