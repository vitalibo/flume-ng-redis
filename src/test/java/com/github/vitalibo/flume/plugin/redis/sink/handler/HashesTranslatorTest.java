package com.github.vitalibo.flume.plugin.redis.sink.handler;

import org.apache.flume.Context;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HashesTranslatorTest {

    private HashesTranslator translator;
    private Context context;

    @BeforeMethod
    public void setUp() {
        context = new Context();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateWithoutPattern() {
        HashesTranslator.from(context);
    }

    @Test
    public void testChooseSequenceNumberStrategy() {
        context.put("redis.hash.pattern", "([a-zA-Z]*) ([0-9]*)");

        translator = HashesTranslator.from(context);

        Assert.assertEquals(translator.getClass(), HashesTranslator.SequenceNumberStrategy.class);
    }

    @Test
    public void testChooseNamedGroupStrategy() {
        context.put("redis.hash.pattern", "(?<name>[a-zA-Z]*) (?<id>[0-9]*)");

        translator = HashesTranslator.from(context);

        Assert.assertEquals(translator.getClass(), HashesTranslator.NamedGroupStrategy.class);
    }

    @Test
    public void testConfigureSequenceNumberStrategy() {
        translator = new HashesTranslator.SequenceNumberStrategy();

        translator.configure(makeSNSContext());

        Assert.assertEquals(
            translator.getPattern().pattern(), "([a-zA-Z]*) ([0-9]*)");
        Assert.assertEquals(
            translator.getFunctions().keySet(), new HashSet<>(Arrays.asList("foo", "bar")));
    }

    @Test
    public void testConfigureNamedGroupStrategy() {
        translator = new HashesTranslator.NamedGroupStrategy();

        translator.configure(makeNGSContext());

        Assert.assertEquals(
            translator.getPattern().pattern(), "(?<foo>[a-zA-Z]*) (?<bar>[0-9]*)");
        Assert.assertEquals(
            translator.getFunctions().keySet(), new HashSet<>(Arrays.asList("foo", "bar")));
    }

    @DataProvider
    public Object[][] sample() {
        return new Object[][]{
            {makeSNSContext(), new HashesTranslator.SequenceNumberStrategy()},
            {makeNGSContext(), new HashesTranslator.NamedGroupStrategy()}
        };
    }

    @Test(dataProvider = "sample")
    public void testStrategy(Context context, HashesTranslator translator) {
        String sample = "Test 01";
        Map<String, String> expected = Stream.of(entry("foo", "Test"), entry("bar", "01"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        translator.configure(context);
        Map<String, String> result = translator.apply(sample);

        Assert.assertEquals(result, expected);
    }

    @Test(dataProvider = "sample", expectedExceptions = IllegalStateException.class)
    public void testParseIncorrectData(Context context, HashesTranslator translator) {
        String sample = "Incorrect data";

        translator.configure(context);
        translator.apply(sample);
    }

    private static Context makeSNSContext() {
        return new Context(Stream
            .of(
                entry("redis.hash.pattern", "([a-zA-Z]*) ([0-9]*)"),
                entry("redis.hash.field.1", "foo"),
                entry("redis.hash.field.2", "bar"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static Context makeNGSContext() {
        return new Context(Stream
            .of(
                entry("redis.hash.pattern", "(?<foo>[a-zA-Z]*) (?<bar>[0-9]*)"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

}
