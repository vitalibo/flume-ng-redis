package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import com.github.vitalibo.flume.plugin.redis.sink.handler.HashesTranslator;
import org.apache.flume.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class RedisHashSinkTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private HashesTranslator mockTranslator;
    @Mock
    private Channel mockChannel;
    @Mock
    private Event mockEvent;

    private RedisHashSink sink;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        sink = new RedisHashSink(mockClient);
        sink.configure(makeContext(), mockTranslator);
    }

    @Test
    public void testConfigure() {
        sink = new RedisHashSink();

        sink.configure(makeContext());

        Assert.assertNotNull(sink.getTranslator());
    }

    @Test
    public void testProcess() throws EventDeliveryException {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockClient.incr(Mockito.anyString())).thenReturn(123L);
        Mockito.when(mockEvent.getBody()).thenReturn("test data".getBytes());
        Mockito.when(mockTranslator.apply("test data")).thenReturn(Collections.singletonMap("key", "foo"));

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.READY);
        Mockito.verify(mockClient).hmset("test:123:id", Collections.singletonMap("key", "foo"));
    }

    @Test
    public void testSkipIncorrectFormat() throws EventDeliveryException {
        Mockito.when(mockChannel.take()).thenReturn(mockEvent);
        Mockito.when(mockClient.incr(Mockito.anyString())).thenReturn(123L);
        Mockito.when(mockEvent.getBody()).thenReturn("test data".getBytes());
        Mockito.when(mockTranslator.apply(Mockito.any())).thenThrow(IllegalStateException.class);

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.BACKOFF);
        Mockito.verify(mockClient, Mockito.never()).hmset(Mockito.anyString(), Mockito.anyMap());
    }

    @Test
    public void testBackoffProcess() throws EventDeliveryException {
        Mockito.when(mockChannel.take()).thenReturn(null);

        Sink.Status status = sink.doProcess(mockChannel);

        Assert.assertEquals(status, Sink.Status.BACKOFF);
        Mockito.verify(mockClient, Mockito.never()).hmset(Mockito.anyString(), Mockito.anyMap());
    }

    private static Context makeContext() {
        Context context = new Context();
        context.put("redis.key", "test:#AUTO_INCREMENT:id");
        context.put("redis.hash.pattern", "([a-zA-Z]*) ([0-9]*)");
        return context;
    }

}
