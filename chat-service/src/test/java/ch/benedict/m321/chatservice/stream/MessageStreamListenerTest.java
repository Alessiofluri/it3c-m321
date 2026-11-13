package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Prüft den Raumfilter aus Spezifikation 3.1.
 *
 * Bewusst ein Unit-Test ohne Broker und ohne Servlet-Umgebung: Geprüft wird
 * eine Entscheidung unseres Codes — welcher Stream eine Nachricht bekommt und
 * welcher nicht. Ein echter Broker würde nur die Laufzeit verlängern und
 * nichts zusätzlich beweisen.
 *
 * Die Emitter sind Attrappen, weil ein echter SseEmitter einen laufenden
 * Request bräuchte. Was sie beweisen, ist genau die Frage dieses Schritts:
 * wurde geschrieben oder nicht.
 */
class MessageStreamListenerTest {

    private final StreamRegistry streamRegistry = new StreamRegistry();

    private final MessageStreamListener listener = new MessageStreamListener(streamRegistry);

    /**
     * Der Kern von Szenario S3: Der Client in Raum B bekommt nichts.
     *
     * Der Fanout gibt dieser Instanz jede Nachricht, auch die für fremde
     * Räume. Ohne den Filter landete eine Nachricht aus Raum A bei jedem
     * verbundenen Client.
     */
    @Test
    void deliversOnlyToTheRoomOfTheMessage() throws Exception {
        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();
        SseEmitter emitterA = mock(SseEmitter.class);
        SseEmitter emitterB = mock(SseEmitter.class);
        StreamConnection connectionA = new StreamConnection(emitterA, roomA);
        StreamConnection connectionB = new StreamConnection(emitterB, roomB);
        streamRegistry.register(connectionA);
        streamRegistry.register(connectionB);
        ChatMessage message = messageForRoom(roomA);

        listener.receive(message);

        verify(emitterA).send(message);
        verifyNoInteractions(emitterB);
    }

    /** Hängen zwei Clients im selben Raum, bekommen beide jede Nachricht. */
    @Test
    void deliversToEveryStreamInTheSameRoom() throws Exception {
        UUID roomA = UUID.randomUUID();
        SseEmitter firstEmitter = mock(SseEmitter.class);
        SseEmitter secondEmitter = mock(SseEmitter.class);
        StreamConnection firstConnection = new StreamConnection(firstEmitter, roomA);
        StreamConnection secondConnection = new StreamConnection(secondEmitter, roomA);
        streamRegistry.register(firstConnection);
        streamRegistry.register(secondConnection);
        ChatMessage message = messageForRoom(roomA);

        listener.receive(message);

        verify(firstEmitter).send(message);
        verify(secondEmitter).send(message);
    }

    /**
     * Ohne Zuhörer wird verworfen, nicht aufbewahrt.
     *
     * Das ist der häufigste Fall im Betrieb: Jede Instanz sieht jede
     * Nachricht, aber nur wenige Räume haben gerade einen offenen Stream.
     * Würde hier etwas geworfen, legte RabbitMQ die Nachricht zurück in die
     * Queue und stellte sie endlos erneut zu.
     */
    @Test
    void dropsMessageWhenNobodyListens() {
        UUID roomWithoutClient = UUID.randomUUID();
        ChatMessage message = messageForRoom(roomWithoutClient);

        assertDoesNotThrow(() -> listener.receive(message));
    }

    /**
     * Ein abgerissener Client darf den Listener nicht umbringen.
     *
     * Tomcat wirft beim Schreiben in eine geschlossene Verbindung eine
     * IOException. Käme die bis zum Container durch, gäbe es kein ACK, der
     * Stapel käme zurück, und alle anderen Clients desselben Raums bekämen
     * die Nachricht ein zweites Mal.
     *
     * Geprüft wird beides: dass nichts nach oben durchkommt, und dass die
     * tote Verbindung aus dem Register verschwindet — sonst scheiterte jede
     * weitere Nachricht dieses Raums an derselben Stelle.
     */
    @Test
    void survivesAClientThatIsGoneAndForgetsIt() throws Exception {
        UUID roomA = UUID.randomUUID();
        SseEmitter deadEmitter = mock(SseEmitter.class);
        IOException clientGone = new IOException("client aborted");
        doThrow(clientGone).when(deadEmitter).send(any(Object.class));
        StreamConnection connection = new StreamConnection(deadEmitter, roomA);
        streamRegistry.register(connection);
        ChatMessage message = messageForRoom(roomA);

        assertDoesNotThrow(() -> listener.receive(message));

        Set<StreamConnection> remaining = streamRegistry.connectionsForRoom(roomA);
        assertTrue(remaining.isEmpty());
    }

    /** Eine Nachricht, wie der chat-service sie veröffentlicht. */
    private ChatMessage messageForRoom(UUID roomId) {
        UUID messageId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new ChatMessage(messageId, roomId, "s1", "Test", "Hallo", sentAt);
    }
}
