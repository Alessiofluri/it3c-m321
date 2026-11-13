package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

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

    private final StreamWriter streamWriter;

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
     * RabbitMQ legte die Nachricht zurück in die Queue und stellte sie
     * erneut zu — der Client bekäme sie doppelt und in falscher Reihenfolge.
     * Reihenfolge ist aber genau das, was Szenario S3 prüft. Gefangen wird
     * jeder Fehler im StreamWriter, eine Ebene tiefer.
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
            streamWriter.sendMessage(connection, message);
        }
    }
}
