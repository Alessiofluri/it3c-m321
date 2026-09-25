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
 * T-5: Szenario S5 in einem Test.
 *
 * Dieselbe Nachricht wird zweimal roh in die Queue gelegt — mit genau
 * einem Header, content_type: application/json, ohne __TypeId__.
 *
 * Geprueft werden damit zwei Dinge auf einmal:
 * 1. Der Dienst kann eine Nachricht lesen, die keine Java-Klasse nennt.
 * 2. Das Duplikat erzeugt keine zweite Zeile und landet nicht im
 *    Abstellgleis chat.dlq.
 *
 * Bewusst mit rabbitTemplate.send statt convertAndSend: convertAndSend
 * wuerde den Header __TypeId__ selbst setzen und damit genau die Frage
 * verstecken, um die es hier geht.
 */
@SpringBootTest
@Testcontainers
class DuplicateMessageIntegrationTest {

    /** Feste ID, damit beide Sendungen wirklich dieselbe Nachricht sind. */
    private static final String MESSAGE_ID = "3f120a73-1922-4577-ae9a-d28063636992";

    private static final String MESSAGE_JSON = """
            {
              "id": "3f120a73-1922-4577-ae9a-d28063636992",
              "roomId": "39acf48b-5d49-40f0-a5d6-cd5bfdb59385",
              "senderId": "anna",
              "senderName": "Anna Muster",
              "content": "Hallo",
              "sentAt": "2026-09-25T13:48:35.746Z"
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
    void sameMessageTwiceResultsInOneRow() {
        Message first = createRawJsonMessage();
        Message second = createRawJsonMessage();

        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, first);
        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, second);

        int rows = waitForRowCount(1);
        assertEquals(1, rows);

        // Auch nach dem Warten darf keine zweite Zeile nachkommen.
        sleepShortly();
        assertEquals(1, countMessages());

        // Ein Duplikat ist kein Fehler: nichts darf im Abstellgleis liegen.
        int deadLettered = countDeadLetterMessages();
        assertEquals(0, deadLettered);
    }

    @Test
    void storesTheMessageContentCorrectly() {
        Message message = createRawJsonMessage();

        rabbitTemplate.send(QueueNames.PERSIST_QUEUE, message);
        waitForRowCount(1);

        String content = jdbcTemplate.queryForObject(
                "SELECT content FROM message WHERE id = ?::uuid", String.class, MESSAGE_ID);
        assertEquals("Hallo", content);
    }

    /**
     * Baut die Nachricht so, wie Szenario S5 sie beschreibt: nur der
     * Inhaltstyp, sonst nichts.
     */
    private Message createRawJsonMessage() {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);

        byte[] body = MESSAGE_JSON.getBytes(StandardCharsets.UTF_8);
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
