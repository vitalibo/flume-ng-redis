package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RedisStringSinkTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private Channel mockChannel;
    @Mock
    private Event mockEvent;

    private RedisStringSink sink;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        sink = new RedisStringSink(mockClient);
        sink.configure(makeContext());
    }

    @Test
    public void testDoProcess() throws Exception {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockClient.incr(Mockito.anyString())).thenReturn(123L);
        Mockito.when(mockEvent.getBody()).thenReturn("test data".getBytes());

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.READY);
        Mockito.verify(mockClient).set("test:123:id", "test data");
    }

    @Test
    public void testBackoffProcess() {
        Mockito.when(mockChannel.take()).thenReturn(null);

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.BACKOFF);
        Mockito.verify(mockClient, Mockito.never()).set(Mockito.anyString(), Mockito.anyString());
    }

    private static Context makeContext() {
        Context context = new Context();
        context.put("redis.key", "test:#AUTO_INCREMENT:id");
        return context;
    }

}
