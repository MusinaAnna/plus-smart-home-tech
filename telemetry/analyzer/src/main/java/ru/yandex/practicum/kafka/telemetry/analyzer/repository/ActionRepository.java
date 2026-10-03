package ru.yandex.practicum.kafka.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Action;

public interface ActionRepository extends JpaRepository<Action, Long> {
}
