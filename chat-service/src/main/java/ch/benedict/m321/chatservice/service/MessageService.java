package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.dto.AcceptedResponse;
import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.dto.SendMessageRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Die Regeln des chat-service.
 *
 * Hier — und nur hier — bekommt eine Nachricht ihre Identität und ihre
 * Zeit. Beides vergibt der Server, damit alle Empfänger dieselbe
 * Reihenfolge und dieselbe ID sehen.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MessageService {

    private final MessagePublisher messagePublisher;

    /**
     * Nimmt eine Nachricht an und gibt zurück, unter welcher ID sie im
     * System unterwegs ist.
     *
     * Der Rückgabewert bedeutet ausdrücklich NICHT "gespeichert" — die
     * Nachricht liegt zu diesem Zeitpunkt erst in der Queue.
     */
    public AcceptedResponse accept(SendMessageRequest request) {
        int contentLength = request.content().length();
        log.info("Message received for room {} with {} characters", request.roomId(), contentLength);

        UUID messageId = UUID.randomUUID();

        // Auf Mikrosekunden gekürzt, weil die Spalte sent_at (timestamptz)
        // nicht genauer speichert. Ohne das Kürzen hätte dieselbe Nachricht
        // live neun Nachkommastellen und aus der Datenbank nur sechs — der
        // Zeitstempel sähe je nach Weg anders aus.
        Instant now = Instant.now();
        Instant sentAt = now.truncatedTo(ChronoUnit.MICROS);

        ChatMessage message = new ChatMessage(
                messageId,
                request.roomId(),
                request.senderId(),
                request.senderName(),
                request.content(),
                sentAt);

        messagePublisher.publish(message);

        return new AcceptedResponse(messageId, sentAt);
    }
}
