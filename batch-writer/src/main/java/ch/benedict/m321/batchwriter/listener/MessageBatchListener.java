package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import ch.benedict.m321.batchwriter.repository.MessageRepository;
import ch.benedict.m321.batchwriter.repository.MessageRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Holt Stapel aus der Queue chat.persist und gibt sie ans Repository.
 *
 * Diese Klasse kennt kein SQL, das Repository kennt kein RabbitMQ. So
 * kann man beide Seiten einzeln testen und weiss bei einem Fehler
 * sofort, wo man suchen muss.
 *
 * Die Bestaetigung an RabbitMQ passiert nicht hier im Code, sondern vom
 * Container, sobald diese Methode ohne Fehler zurueckkehrt. Kehrt sie
 * mit einer Ausnahme zurueck, wird der ganze Stapel nicht bestaetigt und
 * spaeter erneut zugestellt. Das ist At-least-once: nichts geht
 * verloren, Duplikate sind moeglich und werden in der Datenbank
 * abgefangen.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class MessageBatchListener {

    private final MessageRepository messageRepository;

    /**
     * Nimmt einen ganzen Stapel entgegen.
     *
     * Der Parameter ist eine Liste, weil die Listener-Factory mit
     * consumerBatchEnabled arbeitet. Genau aus diesem Parameter leitet
     * Spring AMQP auch den Zieltyp ab — deshalb funktioniert das Lesen
     * ohne den Header __TypeId__.
     *
     * @param messages bis zu 500 Nachrichten, oder weniger nach 200 ms
     */
    @RabbitListener(queues = QueueNames.PERSIST_QUEUE, containerFactory = "batchListenerFactory")
    public void receiveBatch(List<ChatMessage> messages) {
        int received = messages.size();
        log.debug("Received batch of {} messages from queue {}", received, QueueNames.PERSIST_QUEUE);

        List<MessageRow> rows = new ArrayList<>();

        for (ChatMessage message : messages) {
            MessageRow row = toRow(message);
            rows.add(row);
        }

        messageRepository.insertBatch(rows);
    }

    /**
     * Uebersetzt die Nachricht von der Leitung in eine Tabellenzeile.
     *
     * Der Schritt sieht ueberfluessig aus, solange beide gleich
     * aussehen. Er ist die Stelle, an der sich Queue-Format und
     * Tabellenaufbau unabhaengig voneinander aendern duerfen.
     */
    private MessageRow toRow(ChatMessage message) {
        return new MessageRow(
                message.id(),
                message.roomId(),
                message.senderId(),
                message.senderName(),
                message.content(),
                message.sentAt());
    }
}
