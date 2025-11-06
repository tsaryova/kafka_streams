package com.example.kafkastreams.app.order;

import com.example.kafkastreams.domain.ClickEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class ClickAnalyticsStreams {
    public ClickAnalyticsStreams(StreamsBuilder builder, Serde<ClickEvent> clickEventSerde) {
        KStream<String, ClickEvent> clicks = builder.stream("clicks", Consumed.with(Serdes.String(), clickEventSerde));
        clicks
                .groupBy((key, click) -> click.getCategory(), Grouped.with(Serdes.String(), clickEventSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().startTime(),
                        "Category " + windowedKey.key() + ": " + count + " clicks in last 5 min"
                ))
                .to("click-analytics", Produced.with(Serdes.String(), Serdes.String()));
    }
}
