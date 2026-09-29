package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Action;

public interface ActionRepository extends JpaRepository<Action, Long> {
}