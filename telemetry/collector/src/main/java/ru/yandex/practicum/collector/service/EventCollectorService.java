package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Service
public class EventCollectorService {

    private final Producer<String, SpecificRecordBase> kafkaProducer;
    private final String sensorsTopic;
    private final String hubsTopic;

    public EventCollectorService(
            Producer<String, SpecificRecordBase> kafkaProducer,
            @Value("${telemetry.kafka.topics.sensors}") String sensorsTopic,
            @Value("${telemetry.kafka.topics.hubs}") String hubsTopic
    ) {
        this.kafkaProducer = kafkaProducer;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }


    public void collectSensorEvent(SensorEvent event) {
        SensorEventAvro avroEvent = mapSensorEventToAvro(event);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(sensorsTopic, null, avroEvent);
        kafkaProducer.send(record);
    }

    public void collectHubEvent(HubEvent event) {
        HubEventAvro avroEvent = mapHubEventToAvro(event);
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(hubsTopic, null, avroEvent);
        kafkaProducer.send(record);
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

                var avroConditions = scenarioAdded.getConditions().stream()
                        .map(c -> ScenarioConditionAvro.newBuilder()
                                .setSensorId(c.getSensorId())
                                .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                .setValue(c.getValue())
                                .build())
                        .toList();

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