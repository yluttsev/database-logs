package ru.luttsev.databaselogs.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.luttsev.databaselogs.model.KafkaMessage;
import ru.luttsev.databaselogs.repository.KafkaMessageRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLogListener {

    private final KafkaMessageRepository kafkaMessageRepository;

    private ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(id = "logListener", topics = "#{'${kafka.topic.name}'}")
    @Transactional
    public void getMessages(String message) throws JsonProcessingException {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
        KafkaMessage savedMessage = kafkaMessageRepository.save(kafkaMessage);
        log.info("Saved new message: {}", savedMessage.getId());
    }

}
