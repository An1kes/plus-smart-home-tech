package ru.yandex.practicum.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.serialization.SensorsSnapshotDeserializer;
import ru.yandex.practicum.service.ScenarioAnalysisService;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioAnalysisService analysisService;
    private final String snapshotsTopic;

    public SnapshotProcessor(
            @Value("${bootstrap.servers:localhost:9092}") String bootstrapServers,
            @Value("${analyzer.consumer.snapshots.group-id:analyzer-snapshots-group}") String groupId,
            @Value("${telemetry.kafka.topics.snapshots}") String snapshotsTopic,
            ScenarioAnalysisService analysisService
    ) {
        this.snapshotsTopic = snapshotsTopic;
        this.analysisService = analysisService;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            log.info("SnapshotProcessor подписался на топик: {}", snapshotsTopic);
            consumer.subscribe(List.of(snapshotsTopic));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    analysisService.analyzeSnapshot(record.value());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("Остановка SnapshotProcessor");
        } catch (Exception e) {
            log.error("Ошибка в SnapshotProcessor", e);
        } finally {
            consumer.close();
        }
    }
}