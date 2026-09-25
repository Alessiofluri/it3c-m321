package ch.benedict.m321.batchwriter.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Der einzige Weg in die Tabelle message.
 *
 * Hier steckt der Kern des ganzen Dienstes: ein Stapel Nachrichten geht
 * als EIN JDBC-Batch in EINER Transaktion in die Datenbank. Bei 1'667
 * Nachrichten pro Sekunde (PLANUNG.md 4.1) waeren einzelne Transaktionen
 * das Ende der Datenbank.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class MessageRepository {

    /**
     * ON CONFLICT DO NOTHING ist die ganze Duplikat-Behandlung.
     *
     * Kommt dieselbe Nachricht ein zweites Mal, verwirft die Datenbank
     * die Einfuegung still. Kein vorheriges SELECT, kein Merken im
     * Speicher — die Pruefung liegt an der einzigen Stelle, die alle
     * Instanzen gemeinsam sehen.
     *
     * Bewusst DO NOTHING und nicht DO UPDATE: eine einmal gespeicherte
     * Nachricht ist unveraenderlich.
     */
    private static final String INSERT_SQL = """
            INSERT INTO message (id, room_id, sender_id, sender_name, content, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING
            """;

    /**
     * Wie oft ein Stapel hoechstens geschrieben wird, bevor aufgegeben
     * wird.
     *
     * 20 Versuche decken zusammen mit der Wartezeit (1 s, verdoppelnd,
     * hoechstens 5 s) rund 90 Sekunden ab. Das ist an Szenario S7
     * geeicht: Postgres ist dort 15 Sekunden weg.
     */
    private static final int MAX_WRITE_ATTEMPTS = 20;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Schreibt einen ganzen Stapel in einer Transaktion.
     *
     * @Transactional sorgt dafuer, dass entweder alle Zeilen des Stapels
     * ankommen oder keine. Genau darauf baut der Listener: er bestaetigt
     * den Stapel gegenueber RabbitMQ erst, wenn diese Methode ohne
     * Fehler zurueckkehrt.
     *
     * @Retryable faengt den Fall ab, dass die Datenbank kurz weg ist.
     * Wir warten und versuchen es erneut, statt den Stapel wegzuwerfen.
     * Die Reihenfolge stimmt dabei: Spring legt die Wiederholung AUSSEN
     * um die Transaktion, jeder Versuch bekommt also eine frische.
     *
     * Wichtig ist der Unterschied zu einer kaputten Nachricht: Dort ist
     * die NACHRICHT das Problem und Wiederholen hilft nie. Hier ist der
     * EMPFAENGER weg und kommt zurueck.
     *
     * Sind alle Versuche erschoepft, fliegt die Ausnahme weiter. Der
     * Container bestaetigt den Stapel dann nicht, und RabbitMQ stellt
     * ihn spaeter erneut zu. Auch dann geht nichts verloren.
     *
     * @param rows die Zeilen des Stapels, nie leer
     */
    @Retryable(
            retryFor = DataAccessException.class,
            maxAttempts = MAX_WRITE_ATTEMPTS,
            backoff = @Backoff(delay = 1000, multiplier = 2, maxDelay = 5000))
    @Transactional
    public void insertBatch(List<MessageRow> rows) {
        List<Object[]> arguments = new ArrayList<>();

        for (MessageRow row : rows) {
            Object[] values = toArguments(row);
            arguments.add(values);
        }

        jdbcTemplate.batchUpdate(INSERT_SQL, arguments);

        int written = rows.size();
        log.info("Batch of {} messages written to table message", written);
    }

    /**
     * Baut die Werte einer Zeile in der Reihenfolge der Fragezeichen im
     * INSERT.
     *
     * Instant versteht JDBC nicht, deshalb der Umweg ueber Timestamp.
     * Die Spalte ist timestamptz, die Zeitzone geht also nicht verloren.
     */
    private Object[] toArguments(MessageRow row) {
        Timestamp sentAt = Timestamp.from(row.sentAt());

        return new Object[]{
                row.id(),
                row.roomId(),
                row.senderId(),
                row.senderName(),
                row.content(),
                sentAt
        };
    }
}
