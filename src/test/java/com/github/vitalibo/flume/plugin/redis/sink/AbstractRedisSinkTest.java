package com.github.vitalibo.flume.plugin.redis.sink;

import com.github.vitalibo.flume.plugin.redis.RedisClient;
import org.apache.flume.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AbstractRedisSinkTest {

    @Mock
    private RedisClient mockClient;
    @Mock
    private Channel mockChannel;
    @Mock
    private Transaction mockTransaction;

    private AbstractRedisSink spySink;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        spySink = Mockito.spy(new EmbeddedRedisSink(mockClient));
        spySink.setChannel(mockChannel);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
    }

    @Test
    public void testConfigure() {
        Context context = new Context();

        spySink.configure(context);

        Mockito.verify(mockClient).configure(context);
    }

    @Test
    public void testLifeCycle() {
        spySink.configure(new Context());

        spySink.start();
        Mockito.verify(mockClient).openConnection();

        spySink.stop();
        Mockito.verify(mockClient).disconnect();
    }

    @Test
    public void testProcess() throws EventDeliveryException {
        Sink.Status status = spySink.process();

        Assert.assertEquals(status, Sink.Status.READY);
        Mockito.verify(mockTransaction).begin();
        Mockito.verify(mockTransaction).commit();
        Mockito.verify(mockTransaction, Mockito.never()).rollback();
        Mockito.verify(mockTransaction).close();
    }

    @Test
    public void testFailProcess() throws EventDeliveryException {
        Mockito.when(spySink.doProcess(Mockito.any())).thenThrow(Exception.class);

        Sink.Status status = spySink.process();

        Assert.assertEquals(status, Sink.Status.BACKOFF);
        Mockito.verify(mockTransaction).begin();
        Mockito.verify(mockTransaction, Mockito.never()).commit();
        Mockito.verify(mockTransaction).rollback();
        Mockito.verify(mockTransaction).close();
    }

    private static class EmbeddedRedisSink extends AbstractRedisSink {

        EmbeddedRedisSink(RedisClient client) {
            super(client);
        }

        @Override
        public Status doProcess(Channel channel) {
            return Status.READY;
        }

    }

}