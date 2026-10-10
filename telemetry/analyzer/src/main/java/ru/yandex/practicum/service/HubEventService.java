package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional
    public void processHubEvent(HubEventAvro event) {
        String hubId = event.getHubId();
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            handleDeviceAdded(hubId, deviceAdded);
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            handleDeviceRemoved(hubId, deviceRemoved);
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            handleScenarioAdded(hubId, scenarioAdded);
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            handleScenarioRemoved(hubId, scenarioRemoved);
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        log.info("Регистрируем устройство {} в хабе {}", event.getId(), hubId);
        Sensor sensor = Sensor.builder()
                .id(event.getId())
                .hubId(hubId)
                .build();
        sensorRepository.save(sensor);
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        log.info("Удаляем устройство {} из хаба {}", event.getId(), hubId);
        sensorRepository.findByIdAndHubId(event.getId(), hubId)
                .ifPresent(sensorRepository::delete);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        log.info("Добавляем сценарий '{}' для хаба {}", event.getName(), hubId);

        // Ищем существующий или создаем новый
        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .orElseGet(() -> Scenario.builder()
                        .hubId(hubId)
                        .name(event.getName())
                        .build());

        scenario.getConditions().clear();
        scenario.getActions().clear();

        for (ScenarioConditionAvro c : event.getConditions()) {
            Sensor sensor = sensorRepository.findByIdAndHubId(c.getSensorId(), hubId)
                    .orElseGet(() -> sensorRepository.save(Sensor.builder().id(c.getSensorId()).hubId(hubId).build()));

            Integer val = null;
            if (c.getValue() instanceof Integer intVal) {
                val = intVal;
            } else if (c.getValue() instanceof Boolean boolVal) {
                val = boolVal ? 1 : 0;
            }

            Condition condition = conditionRepository.save(Condition.builder()
                    .type(c.getType().name())
                    .operation(c.getOperation().name())
                    .value(val)
                    .build());

            scenario.getConditions().put(sensor, condition);
        }

        for (DeviceActionAvro a : event.getActions()) {
            Sensor sensor = sensorRepository.findByIdAndHubId(a.getSensorId(), hubId)
                    .orElseGet(() -> sensorRepository.save(Sensor.builder().id(a.getSensorId()).hubId(hubId).build()));

            Action action = actionRepository.save(Action.builder()
                    .type(a.getType().name())
                    .value(a.getValue())
                    .build());

            scenario.getActions().put(sensor, action);
        }

        scenarioRepository.save(scenario);
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        log.info("Удаляем сценарий '{}' из хаба {}", event.getName(), hubId);
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenarioRepository::delete);
    }
}