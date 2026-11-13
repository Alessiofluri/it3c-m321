package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Set;
import java.util.UUID;

/**
 * Nimmt jede Nachricht vom Fanout und schreibt sie in die offenen Streams
 * ihres Raums.
 *
 * Das ist die Verbraucherseite des Zustellwegs. Was hier ankommt, geht
 * nirgendwo sonst hin: nicht in die Datenbank, nicht zurück nach
 * chat.persist, nicht an den Exchange. Der Anzeigeweg berührt die Datenbank
 * nie — genau deshalb kommen in Szenario S5 alle Nachrichten beim Client an,
 * obwohl Postgres steht (Spezifikation 3.4).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageStreamListener {

    private final StreamRegistry streamRegistry;

    /**
     * Eine Nachricht aus der eigenen Stream-Queue.
     *
     * Der Queue-Name steht erst zur Laufzeit fest, weil er eine pro Prozess
     * erzeugte UUID enthält. "#{liveQueue.name}" fragt deshalb das Bean aus
     * Schritt 1 nach seinem Namen, statt eine Zeichenkette hinzuschreiben.
     *
     * Der Zieltyp ChatMessage kommt aus der SIGNATUR dieser Methode, nicht
     * aus dem Header __TypeId__. Der Header nennt hier zufällig die eigene
     * Klasse, aber der Vertrag ist das JSON (Spezifikation 2.2). Würde der
     * Header ausgewertet, bräche jeder Dienst, der seine Kopie des Records
     * anders nennt.
     *
     * Diese Methode wirft NIE. Eine Ausnahme würde das ACK verhindern,
     * RabbitMQ legte den Stapel zurück in die Queue und stellte ihn erneut
     * zu — der Client bekäme Nachrichten doppelt und in falscher Reihenfolge.
     * Reihenfolge ist aber genau das, was Szenario S3 prüft.
     */
    @RabbitListener(queues = "#{liveQueue.name}", containerFactory = "streamListenerFactory")
    public void receive(ChatMessage message) {
        UUID roomId = message.roomId();
        Set<StreamConnection> connections = streamRegistry.connectionsForRoom(roomId);
        if (connections.isEmpty()) {
            // Kein Zuhörer in diesem Raum: verwerfen, nicht aufbewahren.
            // Die Nachricht ist über chat.persist auf dem Weg in die
            // Datenbank, und wer sie später sehen will, bekommt sie von
            // dort.
            log.debug("Message {} dropped, no open stream for room {}", message.id(), roomId);
            return;
        }
        for (StreamConnection connection : connections) {
            sendToOneStream(connection, message);
        }
    }

    /**
     * Schreibt eine Nachricht in einen einzelnen Stream.
     *
     * Noch ohne SSE-Ereignisformat: "id:" und "data:" baut Schritt 3, der
     * auch den ObjectMapper dafür hereinholt.
     *
     * Gefangen wird absichtlich jede Ausnahme und nicht nur die IOException.
     * Diese Methode steht zwischen dem Broker und einem fremden Client; was
     * der Client tut, darf den Listener nicht umbringen.
     */
    private void sendToOneStream(StreamConnection connection, ChatMessage message) {
        SseEmitter emitter = connection.emitter();
        try {
            emitter.send(message);
        } catch (Exception sendFailed) {
            handleDeadStream(connection, sendFailed);
        }
    }

    /**
     * Räumt eine Verbindung auf, in die nicht mehr geschrieben werden kann.
     *
     * Ein fehlgeschlagenes Senden ist der einzige Zeitpunkt, an dem ein
     * abgerissener Client auffällt — vorher merkt der Dienst nichts
     * (Spezifikation 3.7.6). Austragen ist dabei wichtiger als Schliessen:
     * Bleibt die Verbindung im Register, versucht jede weitere Nachricht
     * dieses Raums erneut, in sie zu schreiben.
     *
     * completeWithError löst zusätzlich die Callbacks aus, die Schritt 3
     * anmeldet, und gibt den offenen Request bei Tomcat frei. Auch das steht
     * in einem try: Ist der Request schon weg, wirft es — und kein Fehler
     * beim Aufräumen darf den Listener erreichen.
     */
    private void handleDeadStream(StreamConnection connection, Exception cause) {
        UUID roomId = connection.roomId();
        streamRegistry.unregister(connection);
        SseEmitter emitter = connection.emitter();
        try {
            emitter.completeWithError(cause);
        } catch (Exception alreadyGone) {
            log.debug("Emitter for room {} could not be closed: {}", roomId, alreadyGone.getMessage());
        }
        log.warn("Stream for room {} removed after a failed send: {}", roomId, cause.getMessage());
    }
}
