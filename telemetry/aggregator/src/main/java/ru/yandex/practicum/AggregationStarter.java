package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
public class AggregationStarter {

    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecordBase> producer;
    private final SnapshotService snapshotService;
    private final String sensorsTopic;
    private final String snapshotsTopic;

    public AggregationStarter(
            Consumer<String, SensorEventAvro> consumer,
            Producer<String, SpecificRecordBase> producer,
            SnapshotService snapshotService,
            @Value("${telemetry.kafka.topics.sensors}") String sensorsTopic,
            @Value("${telemetry.kafka.topics.snapshots}") String snapshotsTopic
    ) {
        this.consumer = consumer;
        this.producer = producer;
        this.snapshotService = snapshotService;
        this.sensorsTopic = sensorsTopic;
        this.snapshotsTopic = snapshotsTopic;
    }

    public void start() {
        // Добавляем shutdown hook для мягкой остановки консьюмера
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            log.info("Подписываемся на топик событий датчиков: {}", sensorsTopic);
            consumer.subscribe(List.of(sensorsTopic));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    log.info("Получено событие датчика: id={}, hubId={}", event.getId(), event.getHubId());

                    Optional<SensorsSnapshotAvro> snapshotOpt = snapshotService.updateState(event);

                    if (snapshotOpt.isPresent()) {
                        SensorsSnapshotAvro snapshot = snapshotOpt.get();
                        log.info("Состояние изменилось! Отправляем снапшот в топик {}: hubId={}", snapshotsTopic, snapshot.getHubId());

                        ProducerRecord<String, SpecificRecordBase> producerRecord =
                                new ProducerRecord<>(snapshotsTopic, snapshot.getHubId(), snapshot);
                        producer.send(producerRecord);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("Получен сигнал на остановку консьюмера (WakeupException)");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                log.info("Сбрасываем буфер продюсера");
                producer.flush();
                consumer.commitSync();
            } catch (Exception e) {
                log.warn("Ошибка при финальном коммите/сбросе буфера", e);
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }
}