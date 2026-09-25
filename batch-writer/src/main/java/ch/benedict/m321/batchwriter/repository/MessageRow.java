package ch.benedict.m321.batchwriter.repository;

import java.time.Instant;
import java.util.UUID;

/**
 * Eine Zeile der Tabelle message.
 *
 * Bewusst getrennt von ChatMessage aus der Queue: die eine Klasse
 * beschreibt, was auf der Leitung ankommt, die andere, was in der
 * Datenbank steht. Heute sind die Felder gleich. Sobald die Tabelle eine
 * Spalte bekommt, die es in der Nachricht nicht gibt, ist man froh
 * darum.
 *
 * @param id         die vom chat-service vergebene Nachrichten-ID
 * @param roomId     der Raum, in den die Nachricht gehoert
 * @param senderId   sub-Kennung des Absenders aus Keycloak
 * @param senderName Anzeigename, damit die Historie lesbar bleibt
 * @param content    der Text der Nachricht
 * @param sentAt     der vom Server gesetzte Zeitpunkt
 */
public record MessageRow(
        UUID id,
        UUID roomId,
        String senderId,
        String senderName,
        String content,
        Instant sentAt) {
}
