package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.config.QueueNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Eine unvollstaendige Nachricht darf den Stapel nicht mitreissen.
 *
 * Geprueft wird der Fall aus der Spezifikation 3.4: Die kaputte
 * Nachricht landet auf dem Abstellgleis, die gesunde desselben Stapels
 * wird trotzdem geschrieben, und der Dienst laeuft weiter.
 */
@SpringBootTest
@Testcontainers
class IncompleteMessageIntegrationTest {

    /** Dieser Nachricht fehlt der Inhalt — die Tabelle verlangt ihn. */
    private static final String MESSAGE_WITHOUT_CONTENT = """
            {
              "id": "11111111-1111-1111-1111-111111111111",
              "roomId": "39acf48b-5d49-40f0-a5d6-cd5bfdb59385",
              "senderId": "anna",
              "senderName": "Anna Muster",
              "sentAt": "2026-09-25T13:48:35.746Z"
            }
            """;

    private static final String COMPLETE_MESSAGE = """
            {
              "id": "22222222-2222-2222-2222-222222222222",
              "roomId": "39acf48b-5d49-40f0-a5d6-cd5bfdb59385",
              "senderId": "bruno",
              "senderName": "Bruno Muster",
              "content": "Ich bin vollstaendig",
              "sentAt": "2026-09-25T13:48:36.000Z"
            }
            """;

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void emptyQueuesAndTable() {
        rabbitAdmin.purgeQueue(QueueNames.PERSIST_QUEUE);
        rabbitAdmin.purgeQueue(QueueNames.DEAD_LETTER_QUEUE);
        jdbcTemplate.execute("TRUNCATE TABLE message");
    }

    @Test
    void keepsGoodMessageAndMovesIncompleteOneToDeadLetterQueue() {
        Message incomplete = createJsonMessage(MESSAGE_WITHOUT_CONTENT);
        Message complete = createJsonMessage(COMPLETE_MESSAGE);

        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, incomplete);
        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, complete);

        int rows = waitForRowCount(1);
        assertEquals(1, rows, "Die vollstaendige Nachricht haette geschrieben werden muessen");

        String content = jdbcTemplate.queryForObject(
                "SELECT content FROM message", String.class);
        assertEquals("Ich bin vollstaendig", content);

        int deadLettered = waitForDeadLetterCount(1);
        assertEquals(1, deadLettered, "Die unvollstaendige Nachricht haette ins Abstellgleis gehoert");
    }

    /** Baut eine Nachricht mit nur dem Inhaltstyp als Header. */
    private Message createJsonMessage(String json) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);

        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        return new Message(body, properties);
    }

    /** Wartet hoechstens 30 Sekunden auf die erwartete Zeilenzahl. */
    private int waitForRowCount(int expected) {
        int count = 0;

        for (int attempt = 0; attempt < 60; attempt++) {
            count = countMessages();
            if (count >= expected) {
                return count;
            }
            sleepShortly();
        }

        return count;
    }

    /** Wartet hoechstens 30 Sekunden auf das Abstellgleis. */
    private int waitForDeadLetterCount(int expected) {
        int count = 0;

        for (int attempt = 0; attempt < 60; attempt++) {
            count = countDeadLetterMessages();
            if (count >= expected) {
                return count;
            }
            sleepShortly();
        }

        return count;
    }

    /** Eine halbe Sekunde warten. */
    private void sleepShortly() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Zaehlt die Zeilen der Tabelle. */
    private int countMessages() {
        Integer count = jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
        return count;
    }

    /** Zaehlt, was im Abstellgleis liegt. */
    private int countDeadLetterMessages() {
        QueueInformation info = rabbitAdmin.getQueueInfo(QueueNames.DEAD_LETTER_QUEUE);
        return info.getMessageCount();
    }
}
