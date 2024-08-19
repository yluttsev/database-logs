package ru.luttsev.databaselogs.listener;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import ru.luttsev.databaselogs.PostgresTestContainer;
import ru.luttsev.databaselogs.model.KafkaMessage;
import ru.luttsev.databaselogs.repository.KafkaMessageRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@Testcontainers
@Import(PostgresTestContainer.class)
class KafkaLogListenerTests {

    private static String topicName = "test-topic";

    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaMessageRepository kafkaMessageRepository;

    private static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry properties) {
        properties.add("kafka.topic.name", () -> topicName);
        properties.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    static {
        kafkaContainer.start();
    }

    @AfterEach
    public void deleteMessages() {
        kafkaMessageRepository.deleteAll();
    }

    private KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(properties));
    }

    @Test
    @SneakyThrows
    void testSaveLogsInDatabase() {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();
        KafkaMessage testKafkaMessage = KafkaMessage.builder()
                .serviceName("kafka-listener-test-class")
                .message("Test message")
                .build();
        kafkaTemplate.send(topicName, objectMapper.writeValueAsString(testKafkaMessage));

        await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<KafkaMessage> messages = kafkaMessageRepository.findAll();
                            assertFalse(messages.isEmpty());
                            assertEquals(messages.get(0).getMessage(), testKafkaMessage.getMessage());
                        }
                );
    }

}
