package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class SnapshotService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, id -> SensorsSnapshotAvro.newBuilder()
                .setHubId(id)
                .setTimestamp(event.getTimestamp())
                .setSensorsState(new HashMap<>())
                .build());

        Map<String, SensorStateAvro> stateMap = snapshot.getSensorsState();

        if (stateMap.containsKey(sensorId)) {
            SensorStateAvro oldState = stateMap.get(sensorId);

            // Если событие из прошлого (время события строго меньше времени старого состояния)
            if (event.getTimestamp().isBefore(oldState.getTimestamp())) {
                return Optional.empty();
            }

            // Если данные датчика абсолютно те же самые
            if (oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        // Обновляем состояние датчика
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        stateMap.put(sensorId, newState);
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }
}