package com.github.vitalibo.flume.plugin.redis.source;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.exceptions.JedisException;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisListSourceTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private ChannelProcessor mockChannel;

    private RedisListSource source;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        source = new RedisListSource(mockClient, 5, "redis.list.test");
        source.setChannelProcessor(mockChannel);
        source.start();
    }

    @Test
    public void testConfigure() {
        source = new RedisListSource();
        Context context = new Context(Stream.of(entry("database", "1"), entry("key", "test"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        source.configure(context);

        Assert.assertEquals(source.getDatabase(), new Integer(1));
        Assert.assertEquals(source.getKey(), "test");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testDefaultConfiguration() {
        source = new RedisListSource();

        source.configure(new Context());

        Assert.assertTrue(false);
    }

    @Test
    public void testProcess() throws EventDeliveryException {
        Mockito.when(mockClient.rpop(Mockito.anyString())).thenReturn("payload");

        PollableSource.Status status = source.process();

        Assert.assertEquals(status, PollableSource.Status.READY);
        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        Mockito.verify(mockChannel).processEvent(captor.capture());
        Event event = captor.getValue();
        Assert.assertEquals(event.getBody(), "payload".getBytes());
    }

    @Test
    public void testProcessEmptyMessage() throws EventDeliveryException {
        PollableSource.Status status = source.process();

        Mockito.verify(mockChannel, Mockito.never()).processEvent(Mockito.any(Event.class));
        Assert.assertEquals(status, PollableSource.Status.BACKOFF);
    }

    @Test(expectedExceptions = EventDeliveryException.class)
    public void testFailProcessEvent() throws EventDeliveryException {
        Mockito.when(mockClient.rpop(Mockito.anyString())).thenReturn("payload");
        Mockito.doThrow(Exception.class).when(mockChannel).processEvent(Mockito.any(Event.class));

        try {
            source.process();
        } catch (Throwable e) {
            Mockito.verify(mockClient).rpush(Mockito.anyString(), Mockito.eq("payload"));
            throw e;
        }
    }

    @Test
    public void testFailReadMessage() throws EventDeliveryException {
        Mockito.when(mockClient.rpop(Mockito.anyString())).thenThrow(JedisException.class);

        PollableSource.Status status = source.process();

        Mockito.verify(mockClient).disconnect();
        Assert.assertEquals(status, PollableSource.Status.BACKOFF);
    }

    private static Map.Entry<String, String> entry(String key, String value) {
        return new AbstractMap.SimpleEntry<>("redis." + key, value);
    }

}