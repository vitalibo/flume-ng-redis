package com.github.vitalibo.flume.plugin.redis.source;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisSubscribeSourceTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private ChannelProcessor mockChannel;

    private RedisSubscribeSource.RedisSubscriber spySubscriber;
    private RedisSubscribeSource source;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        source = new RedisSubscribeSource(mockClient);
        source.setChannelProcessor(mockChannel);
        spySubscriber = Mockito.spy(source.new RedisSubscriber());
        source.setSubscriber(spySubscriber);
    }

    @Test
    public void testConfigure() {
        source.configure(makeContext("foo,bar"));

        Assert.assertEquals(source.getChannels(), new String[]{"foo", "bar"});
    }

    @Test(
        expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "Redis subscribe channel must be set.")
    public void testFailConfigure() {
        source.configure(new Context());

        Assert.assertTrue(false);
    }

    @Test
    public void testLifeCycle() throws InterruptedException {
        Mockito.doAnswer(answer -> {
            Thread.sleep(5000);
            return answer;
        }).when(mockClient).subscribe(spySubscriber, "foo");
        Mockito.doNothing().when(spySubscriber).unsubscribe();

        source.configure(makeContext("foo"));
        source.start();
        Thread.sleep(10);
        source.stop();
        Thread.sleep(10);

        Mockito.verify(mockClient).configure(Mockito.any(Context.class));
        Mockito.verify(spySubscriber).run();
        Mockito.verify(mockClient, Mockito.times(1)).subscribe(spySubscriber, "foo");
        Mockito.verify(spySubscriber).unsubscribe();
    }

    @Test
    public void testOnMessage() {
        spySubscriber.onMessage("foo", "bar");

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        Mockito.verify(mockChannel).processEvent(captor.capture());
        Event event = captor.getValue();
        Assert.assertEquals(event.getBody(), "bar".getBytes());
    }

    @Test
    public void testDisconnect() {
        source.configure(makeContext("foo"));
        Mockito.doThrow(JedisConnectionException.class).doAnswer(answer -> {
            spySubscriber.interrupt();
            return answer;
        }).when(mockClient).subscribe(spySubscriber, "foo");

        spySubscriber.run();

        Mockito.verify(mockClient).connect();
    }

    @Test(expectedExceptions = Exception.class)
    public void testUnexpectedException() {
        source.configure(makeContext("foo"));
        Mockito.doThrow(Exception.class).when(mockClient).subscribe(spySubscriber, "foo");

        spySubscriber.run();

        Assert.assertFalse(true);
    }

    private static Context makeContext(String channels) {
        Context context = new Context();
        context.put("redis.channels", channels);
        return context;
    }

}
