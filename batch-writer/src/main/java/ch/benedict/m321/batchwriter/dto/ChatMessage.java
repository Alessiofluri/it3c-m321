package ch.benedict.m321.batchwriter.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Die Nachricht, wie sie in der Queue chat.persist liegt.
 *
 * Das ist eine EIGENE Kopie des Records aus dem chat-service, und das
 * ist Absicht. Der Vertrag zwischen den Diensten ist das JSON, nicht
 * eine geteilte Java-Klasse. Ein gemeinsames Modul wuerde alle Dienste
 * aneinanderbinden — dann koennte niemand den chat-service anfassen,
 * ohne den batch-writer neu zu bauen.
 *
 * Folge davon: Der Header __TypeId__ auf der Nachricht zeigt auf die
 * Klasse des chat-service, die es hier gar nicht gibt. Er wird deshalb
 * ignoriert; der Zieltyp kommt aus der Signatur der Listener-Methode
 * (siehe RabbitConfig).
 *
 * @param id         die vom chat-service vergebene Nachrichten-ID
 * @param roomId     der Raum, in den die Nachricht gehoert
 * @param senderId   sub-Kennung des Absenders aus Keycloak
 * @param senderName Anzeigename, damit die Historie lesbar bleibt
 * @param content    der Text der Nachricht
 * @param sentAt     der vom Server gesetzte Zeitpunkt
 */
public record ChatMessage(
        UUID id,
        UUID roomId,
        String senderId,
        String senderName,
        String content,
        Instant sentAt) {
}
