package ch.benedict.m321.chatservice.repository;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Liest die Tabelle message für das Nachliefern beim Neuverbinden.
 *
 * Das ist der EINZIGE Datenbankzugriff des chat-service, und er liest nur.
 * Geschrieben wird ausschliesslich vom batch-writer, dem auch das Schema
 * gehört — deshalb gibt es hier kein Flyway und keine Migration
 * (Spezifikation 3.7.4).
 *
 * JdbcTemplate und kein JPA, wie im batch-writer. Die beiden Abfragen sind
 * der Kern dieses Schritts; eine Abstraktion darüber würde genau das
 * verstecken, was man sehen soll.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class MessageHistoryRepository {

    /**
     * Wie viele Zeilen eine Seite beim Nachliefern hat.
     *
     * Begrenzt den Speicher: Auch nach langer Abwesenheit liegen höchstens
     * 500 Zeilen gleichzeitig im Hauptspeicher, egal wie viel der Client
     * verpasst hat.
     */
    public static final int PAGE_SIZE = 500;

    /**
     * Sucht den Zeitstempel des Ankers.
     *
     * Die room_id steht absichtlich in der Bedingung: Eine Last-Event-ID
     * aus einem FREMDEN Raum darf nicht gefunden werden, sonst lieferte der
     * Dienst ab einem Zeitpunkt nach, der mit diesem Raum nichts zu tun
     * hat.
     */
    private static final String ANCHOR_SQL = """
            SELECT sent_at
            FROM message
            WHERE id = ?
              AND room_id = ?
            """;

    /**
     * Holt eine Seite nach dem Anker (Spezifikation 3.7.3).
     *
     * Zwei Bedingungen über sent_at, und beide sind nötig:
     *
     * `sent_at >= ?` ist für den INDEX da. Der vorhandene Index lautet
     * (room_id, sent_at DESC) und enthält id nicht; ohne eine Bedingung
     * über sent_at allein könnte Postgres ihn nicht zum Eingrenzen nutzen.
     *
     * `(sent_at, id) > (?, ?)` ist für die KORREKTHEIT da. Die Tabelle hat
     * keine Sequenznummer, "nach dieser Nachricht" heisst also: nach dem
     * Tupel aus Zeitstempel und ID. Bei gleicher Zeitangabe bricht die ID
     * den Gleichstand — ihre Ordnung ist willkürlich, aber stabil, und
     * mehr braucht es nicht: keine Zeile doppelt, keine übersprungen.
     *
     * Allein mit `sent_at >= ?` käme der Anker selbst noch einmal mit.
     * Allein mit `sent_at > ?` fielen Nachrichten weg, die in derselben
     * Mikrosekunde entstanden sind.
     */
    private static final String PAGE_SQL = """
            SELECT id, room_id, sender_id, sender_name, content, sent_at
            FROM message
            WHERE room_id = ?
              AND sent_at >= ?
              AND (sent_at, id) > (?, ?)
            ORDER BY sent_at, id
            LIMIT ?
            """;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Der Zeitstempel der Nachricht, bei der ein Client stehen geblieben
     * ist.
     *
     * query statt queryForObject: queryForObject wirft, wenn nichts
     * gefunden wird. "Nicht gefunden" ist hier aber der normale Fall —
     * entweder hat der batch-writer die Nachricht noch nicht geschrieben,
     * oder die ID gehört zu einem anderen Raum. Eine Ausnahme für einen
     * erwarteten Fall wäre irreführend.
     */
    public Optional<Instant> findSentAt(UUID messageId, UUID roomId) {
        List<Instant> found = jdbcTemplate.query(ANCHOR_SQL, this::mapSentAt, messageId, roomId);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        Instant sentAt = found.get(0);
        return Optional.of(sentAt);
    }

    /**
     * Eine Seite Nachrichten nach dem Anker, aufsteigend.
     *
     * Weitergeblättert wird per Keyset: Die letzte Zeile der Seite ist der
     * neue Anker. Kein OFFSET, denn das müsste bei jeder Seite alle
     * vorherigen Zeilen erneut durchlaufen.
     */
    public List<ChatMessage> findPageAfter(UUID roomId, Instant anchorSentAt, UUID anchorId) {
        OffsetDateTime anchorTime = anchorSentAt.atOffset(ZoneOffset.UTC);
        return jdbcTemplate.query(PAGE_SQL, this::mapMessage, roomId, anchorTime, anchorTime, anchorId, PAGE_SIZE);
    }

    /** Liest nur die Spalte sent_at, für die Ankersuche. */
    private Instant mapSentAt(ResultSet row, int rowNumber) throws SQLException {
        OffsetDateTime sentAt = row.getObject("sent_at", OffsetDateTime.class);
        return sentAt.toInstant();
    }

    /**
     * Baut aus einer Zeile denselben Record, den der Publisher verschickt.
     *
     * sent_at wird als OffsetDateTime gelesen und zu Instant gemacht, NIE
     * als Zeichenkette formatiert. Mit getString oder Timestamp.toString
     * stünde im nachgelieferten JSON "2026-11-13 09:09:09.251491" statt
     * "2026-11-13T09:09:09.251491Z" — live und nachgeliefert sähen
     * verschieden aus, und genau diese Abweichung sollte die Kürzung auf
     * Mikrosekunden aus Commit 52a2ea3 verhindern.
     */
    private ChatMessage mapMessage(ResultSet row, int rowNumber) throws SQLException {
        UUID id = row.getObject("id", UUID.class);
        UUID roomId = row.getObject("room_id", UUID.class);
        String senderId = row.getString("sender_id");
        String senderName = row.getString("sender_name");
        String content = row.getString("content");
        OffsetDateTime sentAtWithOffset = row.getObject("sent_at", OffsetDateTime.class);
        Instant sentAt = sentAtWithOffset.toInstant();
        return new ChatMessage(id, roomId, senderId, senderName, content, sentAt);
    }
}
