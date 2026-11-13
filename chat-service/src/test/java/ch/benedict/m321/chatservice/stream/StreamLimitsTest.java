package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.repository.MessageHistoryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Prüft die Grenzen aus Spezifikation 3.7.7.
 *
 * Beide Grenzen schliessen den Stream, statt eine Nachricht zu verwerfen.
 * Der Unterschied ist entscheidend: Beim Schliessen verbindet sich der
 * Browser ab seiner letzten WIRKLICH erhaltenen ID neu und die
 * Nachlieferung beginnt dort. Würde verworfen, fehlte dem Client die
 * Nachricht für immer, ohne dass es jemand merkt.
 */
class StreamLimitsTest {

    private static final int MAX_PENDING = 1000;

    private static final int MAX_CATCH_UPS = 200;

    private final StreamRegistry streamRegistry = new StreamRegistry();

    private final StreamWriter streamWriter = new StreamWriter(objectMapperWithInstantSupport(), streamRegistry);

    private final StreamDelivery streamDelivery = new StreamDelivery(
            streamWriter,
            mock(MessageHistoryRepository.class),
            neverRunningScheduler());

    /**
     * Die 1001. Nachricht schliesst den Stream.
     *
     * Die ersten 1000 landen im Zwischenspeicher, ohne dass gesendet wird —
     * die Verbindung ist CATCHING_UP. Erst die eine zu viel löst das
     * Schliessen aus.
     */
    @Test
    void closesTheStreamWhenTheBufferIsFull() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = new StreamConnection(emitter, roomId, Instant.now(), StreamState.CATCHING_UP);
        streamRegistry.register(connection);

        for (int i = 0; i < MAX_PENDING; i = i + 1) {
            ChatMessage message = messageIn(roomId, "Nummer " + i);
            streamDelivery.deliverLive(connection, message);
        }

        verify(emitter, never()).complete();
        synchronized (connection.lock()) {
            assertEquals(MAX_PENDING, connection.pendingCount());
        }

        ChatMessage oneTooMany = messageIn(roomId, "eine zu viel");
        streamDelivery.deliverLive(connection, oneTooMany);

        verify(emitter).complete();
        List<StreamConnection> remaining = streamRegistry.allConnections();
        assertTrue(remaining.isEmpty(), "Die Verbindung muss ausgetragen sein");
    }

    /**
     * Die 201. Nachlieferung wird abgewiesen.
     *
     * Geprüft wird auch der Zähler: Er darf durch die Abweisung nicht
     * steigen, sonst wären nach ein paar Abweisungen alle Plätze dauerhaft
     * belegt.
     */
    @Test
    void refusesTheCatchUpBeyondTheLimit() throws Exception {
        UUID roomId = UUID.randomUUID();
        for (int i = 0; i < MAX_CATCH_UPS; i = i + 1) {
            SseEmitter accepted = mock(SseEmitter.class);
            StreamConnection connection = catchingUp(accepted, roomId);
            streamDelivery.scheduleCatchUp(connection, UUID.randomUUID());
            verify(accepted, never()).complete();
        }
        assertEquals(MAX_CATCH_UPS, streamDelivery.runningCatchUps());

        SseEmitter refused = mock(SseEmitter.class);
        StreamConnection tooLate = catchingUp(refused, roomId);
        streamRegistry.register(tooLate);
        streamDelivery.scheduleCatchUp(tooLate, UUID.randomUUID());

        verify(refused).complete();
        assertEquals(MAX_CATCH_UPS, streamDelivery.runningCatchUps(), "Der Zaehler darf nicht gestiegen sein");
    }

    private StreamConnection catchingUp(SseEmitter emitter, UUID roomId) {
        return new StreamConnection(emitter, roomId, Instant.now(), StreamState.CATCHING_UP);
    }

    private ChatMessage messageIn(UUID roomId, String content) {
        UUID messageId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new ChatMessage(messageId, roomId, "s1", "Test", content, sentAt);
    }

    /**
     * Ein echter Scheduler mit einem einzigen Thread.
     *
     * Die eingeplanten Aufgaben starten erst nach der Sekunde Verzögerung
     * aus StreamDelivery und laufen dann einzeln hintereinander. Dieser
     * Test prüft in Mikrosekunden, der Zähler bleibt also für die Dauer der
     * Prüfung oben — genau das wird gebraucht, denn eine sofort laufende
     * Aufgabe würde ihren Platz sofort wieder freigeben und die Grenze wäre
     * nicht erreichbar.
     *
     * Was die Aufgaben später tun, ist belanglos: Das Repository ist eine
     * Attrappe, der Anker wird nie gefunden, und die Verbindungen schalten
     * auf live. Die Nachlieferung selbst hat ihren eigenen Test.
     */
    private static ThreadPoolTaskScheduler neverRunningScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.initialize();
        return scheduler;
    }

    private static ObjectMapper objectMapperWithInstantSupport() {
        ObjectMapper objectMapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        objectMapper.registerModule(javaTimeModule);
        return objectMapper;
    }
}
