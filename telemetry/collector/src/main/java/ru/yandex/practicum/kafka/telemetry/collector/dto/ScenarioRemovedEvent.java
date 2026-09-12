package ru.yandex.practicum.kafka.telemetry.collector.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ScenarioRemovedEvent extends HubEvent {
    private String name;

    @Override
    public String getType() {
        return "SCENARIO_REMOVED";
    }
}
