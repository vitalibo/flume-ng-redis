package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RedisPublishSinkTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private Channel mockChannel;
    @Mock
    private Event mockEvent;

    private RedisPublishSink sink;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        sink = new RedisPublishSink(mockClient);
        sink.configure(makeContext());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConfigure() {
        sink = new RedisPublishSink(mockClient);

        sink.configure(new Context());
    }

    @Test
    public void testProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockEvent.getBody()).thenReturn("foo bar".getBytes());
        Mockito.when(mockClient.publish(Mockito.anyString(), Mockito.anyString())).thenReturn(1L);

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.READY);
        Mockito.verify(mockClient).publish("demo", "foo bar");
        Mockito.verify(mockClient).publish("test", "foo bar");
    }

    @Test
    public void testBackoffProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(null);

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.BACKOFF);
        Mockito.verify(mockClient, Mockito.never()).publish(Mockito.anyString(), Mockito.anyString());
    }

    @Test(expectedExceptions = EventDeliveryException.class)
    public void testFailProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockEvent.getBody()).thenReturn("foo bar".getBytes());
        Mockito.when(mockClient.publish(Mockito.anyString(), Mockito.anyString())).thenReturn(0L);

        sink.doProcess(mockChannel);

        Assert.assertTrue(false);
    }

    private static Context makeContext() {
        Context context = new Context();
        context.put("redis.channels", "demo, test");
        return context;
    }

}