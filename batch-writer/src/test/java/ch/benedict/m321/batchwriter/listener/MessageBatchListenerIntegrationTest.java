package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * T-3: der Weg von der Queue bis in die Tabelle, mit echtem Broker und
 * echter Datenbank.
 *
 * Das ist der Test, der Szenario S3 absichert. Ein nachgebauter Broker
 * wuerde genau die Fehler verschweigen, die hier interessant sind —
 * zum Beispiel ein Stapel, der nie voll wird.
 */
@SpringBootTest
@Testcontainers
class MessageBatchListenerIntegrationTest {

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

    /** Queue und Tabelle sind gemeinsamer Zustand ueber alle Tests. */
    @BeforeEach
    void emptyQueueAndTable() {
        rabbitAdmin.purgeQueue(QueueNames.PERSIST_QUEUE);
        jdbcTemplate.execute("TRUNCATE TABLE message");
    }

    @Test
    void writesEveryMessageFromQueueIntoTable() {
        int expected = 50;

        for (int i = 0; i < expected; i++) {
            ChatMessage message = createMessage("Nachricht " + i);
            rabbitTemplate.convertAndSend(QueueNames.PERSIST_QUEUE, message);
        }

        int written = waitForRowCount(expected);
        assertEquals(expected, written);
    }

    /**
     * Wartet, bis die erwartete Zeilenzahl erreicht ist, hoechstens aber
     * 30 Sekunden.
     *
     * Warten ist noetig, weil der Listener nebenlaeufig arbeitet: der
     * Test sendet, der Stapel wird erst nach 200 ms oder 500 Stueck
     * geschrieben. Ein festes Thread.sleep waere entweder zu kurz und
     * damit unzuverlaessig, oder zu lang und damit langsam.
     */
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

    /** Eine halbe Sekunde warten, ohne den Test mit try/catch zu fluten. */
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

    /** Baut eine vollstaendige Nachricht mit eigener ID. */
    private ChatMessage createMessage(String content) {
        UUID messageId = UUID.randomUUID();
        UUID roomId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new ChatMessage(messageId, roomId, "anna", "Anna Muster", content, sentAt);
    }
}
