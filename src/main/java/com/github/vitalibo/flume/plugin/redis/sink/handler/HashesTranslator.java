package com.github.vitalibo.flume.plugin.redis.sink.handler;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@NoArgsConstructor
public abstract class HashesTranslator implements Function<String, Map<String, String>>, Configurable {

    @Getter
    protected Pattern pattern;
    @Getter
    protected Map<String, Function<Matcher, String>> functions;

    @Override
    public Map<String, String> apply(String value) {
        Matcher matcher = pattern.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalStateException("No match found");
        }

        return functions.entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey, (e) -> e.getValue().apply(matcher)));
    }

    public static HashesTranslator from(Context context) {
        String pattern = context.getString("redis.hash.pattern");
        Preconditions.checkNotNull(pattern, "Redis hash pattern mast be set.");

        HashesTranslator translator = chooseStrategy(pattern);
        translator.configure(context);

        return translator;
    }

    private static HashesTranslator chooseStrategy(String pattern) {
        Matcher matcher = NamedGroupStrategy.PATTERN.matcher(pattern);
        if (matcher.find()) {
            return new NamedGroupStrategy();
        }

        return new SequenceNumberStrategy();
    }

    static class NamedGroupStrategy extends HashesTranslator {

        private static final Pattern PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

        @Override
        public void configure(Context context) {
            String pattern = context.getString("redis.hash.pattern");
            Matcher matcher = PATTERN.matcher(pattern);

            this.pattern = Pattern.compile(pattern);
            Map<String, Function<Matcher, String>> functions = new HashMap<>();
            while (matcher.find()) {
                String group = matcher.group(1);
                functions.put(group, m -> m.group(group));
            }

            this.functions = Collections.unmodifiableMap(functions);
        }
    }

    static class SequenceNumberStrategy extends HashesTranslator {

        @Override
        public void configure(Context context) {
            pattern = Pattern.compile(context.getString("redis.hash.pattern"));

            Map<String, Function<Matcher, String>> functions = new HashMap<>();
            for (int i = 1; i < Integer.MAX_VALUE; i++) {
                String name = context.getString("redis.hash.field." + i);
                if (name == null) {
                    this.functions = Collections.unmodifiableMap(functions);
                    break;
                }

                final int group = i;
                functions.put(name, m -> m.group(group));
            }
        }

    }

}
