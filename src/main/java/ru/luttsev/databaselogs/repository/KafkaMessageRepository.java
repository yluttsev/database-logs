package ru.luttsev.databaselogs.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.luttsev.databaselogs.model.KafkaMessage;

import java.util.UUID;

@Repository
public interface KafkaMessageRepository extends JpaRepository<KafkaMessage, UUID> {
}
