package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class ScenarioAddedEvent extends HubEvent {
    private String name;
    private List<ScenarioCondition> conditions;
    private List<DeviceAction> actions;

    @Override
    public String getType() {
        return "SCENARIO_ADDED";
    }
}
