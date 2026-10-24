package ru.yandex.practicum.kafka.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Scenario;

import java.util.List;
import java.util.Optional;

public interface ScenarioRepository extends JpaRepository<Scenario, Long> {

    @Query("select distinct s " +
            "from Scenario s " +
            "left join fetch s.conditions " +
            "left join fetch s.actions " +
            "where s.hubId = ?1")
    List<Scenario> findByHubId(String hubId);

    @Query("select distinct s " +
            "from Scenario s " +
            "left join fetch s.conditions " +
            "left join fetch s.actions " +
            "where s.hubId = ?1 and s.name = ?2")
    Optional<Scenario> findByHubIdAndName(String hubId, String name);
}
