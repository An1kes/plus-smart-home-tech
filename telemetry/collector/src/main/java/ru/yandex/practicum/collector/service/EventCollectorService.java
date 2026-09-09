package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class EventCollectorService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    public void collectSensorEvent(SensorEvent event) {

        System.out.println("PROCESSING SENSOR EVENT: id=" + event.getId() + ", type=" + event.getType());
        SensorEventAvro avroEvent = mapSensorEventToAvro(event);
        byte[] serializedData = serialize(avroEvent);

        kafkaTemplate.send(SENSORS_TOPIC, null, serializedData);
    }

    private SensorEventAvro mapSensorEventToAvro(SensorEvent event) {
        Object payload = null;

        switch (event.getType()) {
            case LIGHT_SENSOR_EVENT:
                LightSensorEvent lightSrc = (LightSensorEvent) event;
                payload = LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSrc.getLinkQuality())
                        .setLuminosity(lightSrc.getLuminosity())
                        .build();
                break;
            case CLIMATE_SENSOR_EVENT:
                ClimateSensorEvent climateSrc = (ClimateSensorEvent) event;
                payload = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateSrc.getTemperatureC())
                        .setHumidity(climateSrc.getHumidity())
                        .setCo2Level(climateSrc.getCo2Level())
                        .build();
                break;
            case MOTION_SENSOR_EVENT:
                MotionSensorEvent motionSrc = (MotionSensorEvent) event;
                payload = MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSrc.getLinkQuality())
                        .setMotion(motionSrc.isMotion())
                        .setVoltage(motionSrc.getVoltage())
                        .build();
                break;
            case SWITCH_SENSOR_EVENT:
                SwitchSensorEvent switchSrc = (SwitchSensorEvent) event;
                payload = SwitchSensorAvro.newBuilder()
                        .setState(switchSrc.isState())
                        .build();
                break;
            case TEMPERATURE_SENSOR_EVENT:
                TemperatureSensorEvent tempSrc = (TemperatureSensorEvent) event;
                payload = TemperatureSensorAvro.newBuilder()
                        .setId(tempSrc.getId())
                        .setHubId(tempSrc.getHubId())
                        .setTimestamp(tempSrc.getTimestamp() != null ? tempSrc.getTimestamp() : Instant.now())
                        .setTemperatureC(tempSrc.getTemperatureC())
                        .setTemperatureF(tempSrc.getTemperatureF())
                        .build();
                break;
        }

        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp() != null ? event.getTimestamp() : Instant.now())
                .setPayload(payload)
                .build();
    }

    private byte[] serialize(SpecificRecordBase record) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            SpecificDatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(record.getSchema());
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro record", e);
        }
    }

    public void collectHubEvent(HubEvent event) {
        System.out.println("PROCESSING HUB EVENT: id=" + event + ", type=" + event.getType());
        HubEventAvro avroEvent = mapHubEventToAvro(event);
        byte[] serializedData = serialize(avroEvent);

        kafkaTemplate.send(HUBS_TOPIC, null, serializedData);
    }

    private HubEventAvro mapHubEventToAvro(HubEvent event) {
        Object payload = null;

        switch (event.getType()) {
            case DEVICE_ADDED:
                DeviceAddedEvent addedSrc = (DeviceAddedEvent) event;
                payload = DeviceAddedEventAvro.newBuilder()
                        .setId(addedSrc.getId())
                        .setType(DeviceTypeAvro.valueOf(addedSrc.getDeviceType().name()))
                        .build();
                break;
            case DEVICE_REMOVED:
                DeviceRemovedEvent removedSrc = (DeviceRemovedEvent) event;
                payload = DeviceRemovedEventAvro.newBuilder()
                        .setId(removedSrc.getId())
                        .build();
                break;
            case SCENARIO_ADDED:
                ScenarioAddedEvent scenarioAdded = (ScenarioAddedEvent) event;

                // Мапим условия
                var avroConditions = scenarioAdded.getConditions().stream()
                        .map(c -> ScenarioConditionAvro.newBuilder()
                                .setSensorId(c.getSensorId())
                                .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                .setValue(c.getValue())
                                .build())
                        .toList();

                // Мапим действия
                var avroActions = scenarioAdded.getActions().stream()
                        .map(a -> DeviceActionAvro.newBuilder()
                                .setSensorId(a.getSensorId())
                                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                .setValue(a.getValue())
                                .build())
                        .toList();

                payload = ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAdded.getName())
                        .setConditions(avroConditions)
                        .setActions(avroActions)
                        .build();
                break;
            case SCENARIO_REMOVED:
                ScenarioRemovedEvent scenarioRemoved = (ScenarioRemovedEvent) event;
                payload = ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemoved.getName())
                        .build();
                break;
        }

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp() != null ? event.getTimestamp() : Instant.now())
                .setPayload(payload)
                .build();
    }
}