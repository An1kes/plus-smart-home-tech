package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ScenarioAnalysisService {

    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public ScenarioAnalysisService(
            ScenarioRepository scenarioRepository,
            @GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient
    ) {
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    @Transactional(readOnly = true)
    public void analyzeSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        log.info("АНАЛИЗ СНАПШОТА: hubId={}, найдено сценариев в базе: {}", hubId, scenarios.size());

        if (scenarios.isEmpty()) {
            return;
        }

        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        for (Scenario scenario : scenarios) {
            boolean applicable = isScenarioApplicable(scenario, sensorsState);
            log.info("Сценарий '{}' применим? {}", scenario.getName(), applicable);

            if (applicable) {
                log.info("Сценарий '{}' сработал для хаба '{}'! Отправляем действия.", scenario.getName(), hubId);
                executeActions(scenario, hubId);
            }
        }
    }

    private boolean isScenarioApplicable(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        if (scenario.getConditions().isEmpty()) {
            log.warn("У сценария '{}' нет условий в БД!", scenario.getName());
            return false;
        }

        for (Map.Entry<Sensor, Condition> entry : scenario.getConditions().entrySet()) {
            String sensorId = entry.getKey().getId();
            Condition condition = entry.getValue();

            SensorStateAvro state = sensorsState.get(sensorId);
            if (state == null) {
                log.info("Сценарий '{}': в снапшоте НЕТ датчика {}", scenario.getName(), sensorId);
                return false;
            }

            boolean conditionPassed = checkCondition(condition, state.getData());
            log.info("Сценарий '{}': датчик {}, тип условия {}, операция {}, targetValue={}, actualData={}, результат={}",
                    scenario.getName(), sensorId, condition.getType(), condition.getOperation(),
                    condition.getValue(), state.getData(), conditionPassed);

            if (!conditionPassed) {
                return false;
            }
        }

        return true;
    }

    private boolean checkCondition(Condition condition, Object sensorData) {
        Integer actualValue = extractValue(condition.getType(), sensorData);
        if (actualValue == null) {
            return false;
        }

        Integer targetValue = condition.getValue();
        if (targetValue == null) {
            return false;
        }

        return switch (condition.getOperation()) {
            case "EQUALS" -> actualValue.equals(targetValue);
            case "GREATER_THAN" -> actualValue > targetValue;
            case "LOWER_THAN" -> actualValue < targetValue;
            default -> false;
        };
    }

    private Integer extractValue(String conditionType, Object sensorData) {
        if (sensorData instanceof ClimateSensorAvro climate) {
            return switch (conditionType) {
                case "TEMPERATURE" -> climate.getTemperatureC();
                case "HUMIDITY" -> climate.getHumidity();
                case "CO2LEVEL" -> climate.getCo2Level();
                default -> null;
            };
        } else if (sensorData instanceof LightSensorAvro light) {
            if ("LUMINOSITY".equals(conditionType)) {
                return light.getLuminosity();
            }
        } else if (sensorData instanceof MotionSensorAvro motion) {
            if ("MOTION".equals(conditionType)) {
                return motion.getMotion() ? 1 : 0;
            }
        } else if (sensorData instanceof SwitchSensorAvro switchSensor) {
            if ("SWITCH".equals(conditionType)) {
                return switchSensor.getState() ? 1 : 0;
            }
        } else if (sensorData instanceof TemperatureSensorAvro temp) {
            if ("TEMPERATURE".equals(conditionType)) {
                return temp.getTemperatureC();
            }
        }
        return null;
    }

    private void executeActions(Scenario scenario, String hubId) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        for (Map.Entry<Sensor, Action> entry : scenario.getActions().entrySet()) {
            String sensorId = entry.getKey().getId();
            Action action = entry.getValue();

            DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(sensorId)
                    .setType(ActionTypeProto.valueOf(action.getType()));

            if (action.getValue() != null) {
                actionBuilder.setValue(action.getValue());
            }

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenario.getName())
                    .setAction(actionBuilder.build())
                    .setTimestamp(timestamp)
                    .build();

            // Отправляем с 3 попытками, если сервер еще не успел открыть порт
            boolean sent = false;
            for (int attempt = 1; attempt <= 3 && !sent; attempt++) {
                try {
                    hubRouterClient.handleDeviceAction(request);
                    log.info("ДЕЙСТВИЕ УСПЕШНО ОТПРАВЛЕНО В HUB ROUTER: {}", request);
                    sent = true;
                } catch (Exception e) {
                    log.warn("Попытка {}/3 отправки в Hub Router не удалась: {}. Повтор через 500мс...", attempt, e.getMessage());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }
}