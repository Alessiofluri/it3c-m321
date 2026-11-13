package ch.benedict.m321.chatservice.repository;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft die Nachliefer-Abfrage aus Spezifikation 3.7.3 gegen eine ECHTE
 * Postgres.
 *
 * Ein Mock wäre hier sinnlos: Geprüft wird, was die Datenbank mit dem
 * Tupelvergleich `(sent_at, id) > (?, ?)` tut, wie sie bei gleicher
 * Zeitangabe sortiert, und ob ein timestamptz-Wert den Weg hin und zurück
 * unverändert übersteht. Alles drei sind Eigenschaften von Postgres und des
 * JDBC-Treibers, nicht unseres Codes.
 *
 * Das Schema wird aus der Migration des batch-writer gelesen, über den
 * relativen Pfad. Eine Kopie hier im Test wäre bequemer, würde aber
 * irgendwann von der echten Tabelle abweichen — und dann prüfte dieser Test
 * eine Tabelle, die es nicht gibt.
 *
 * Kein Spring-Kontext: Gebraucht wird nur ein JdbcTemplate. Das spart den
 * RabbitMQ-Container, den @SpringBootTest mitbringen müsste.
 */
@Testcontainers
class MessageHistoryRepositoryIntegrationTest {

    /** Der Pfad gilt relativ zum Modulverzeichnis chat-service. */
    private static final String SCHEMA_PATH =
            "../batch-writer/src/main/resources/db/migration/V1__create_message_table.sql";

    private static final String INSERT_SQL = """
            INSERT INTO message (id, room_id, sender_id, sender_name, content, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    private static JdbcTemplate jdbcTemplate;

    private static MessageHistoryRepository repository;

    @BeforeAll
    static void createSchema() throws Exception {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        jdbcTemplate = new JdbcTemplate(dataSource);
        repository = new MessageHistoryRepository(jdbcTemplate);

        Path schemaFile = Path.of(SCHEMA_PATH);
        String schema = Files.readString(schemaFile);
        jdbcTemplate.execute(schema);
    }

    /** Der Anker wird über ID UND Raum gesucht. */
    @Test
    void findsTheTimestampOfTheAnchor() {
        UUID roomId = UUID.randomUUID();
        ChatMessage anchor = messageIn(roomId, Instant.parse("2026-11-13T10:00:00.123456Z"), "Anker");
        insert(anchor);

        Optional<Instant> found = repository.findSentAt(anchor.id(), roomId);

        assertTrue(found.isPresent());
        assertEquals(anchor.sentAt(), found.get());
    }

    /**
     * Eine Last-Event-ID aus einem fremden Raum wird nicht gefunden.
     *
     * Ohne die room_id in der Bedingung lieferte der Dienst ab einem
     * Zeitpunkt nach, der mit dem angefragten Raum nichts zu tun hat — der
     * Client bekäme eine zufällige Scheibe seiner eigenen Historie.
     */
    @Test
    void doesNotFindAnAnchorFromAnotherRoom() {
        UUID ownRoom = UUID.randomUUID();
        UUID otherRoom = UUID.randomUUID();
        ChatMessage inOtherRoom = messageIn(otherRoom, Instant.parse("2026-11-13T10:01:00Z"), "fremd");
        insert(inOtherRoom);

        Optional<Instant> found = repository.findSentAt(inOtherRoom.id(), ownRoom);

        assertFalse(found.isPresent());
    }

    /**
     * Blättern über mehr als eine Seite: 1200 Zeilen, Seiten von 500.
     *
     * Geprüft wird das Ergebnis des ganzen Durchlaufs — alle 1199
     * Nachrichten nach dem Anker, in aufsteigender Reihenfolge, keine
     * doppelt und keine übersprungen.
     */
    @Test
    void pagesThroughMoreRowsThanOnePageHolds() {
        UUID roomId = UUID.randomUUID();
        Instant start = Instant.parse("2026-11-13T11:00:00Z");
        List<ChatMessage> written = new ArrayList<>();
        for (int i = 0; i < 1200; i = i + 1) {
            Instant sentAt = start.plus(i, ChronoUnit.MILLIS);
            ChatMessage message = messageIn(roomId, sentAt, "Nachricht " + i);
            insert(message);
            written.add(message);
        }
        ChatMessage anchor = written.get(0);

        List<ChatMessage> delivered = readEverythingAfter(roomId, anchor);

        assertEquals(1199, delivered.size());
        assertNoDuplicates(delivered);
        assertAscending(delivered);
        ChatMessage firstDelivered = delivered.get(0);
        ChatMessage expectedFirst = written.get(1);
        assertEquals(expectedFirst, firstDelivered);
    }

    /**
     * Gleichstand bei sent_at: Die ID entscheidet, und keine Zeile geht
     * verloren.
     *
     * Das ist der Fall, für den die Tabelle keine Sequenznummer hat. Fünf
     * Nachrichten mit identischer Zeitangabe — mit `sent_at > ?` allein
     * fielen vier davon weg, mit `sent_at >= ?` allein käme der Anker
     * doppelt.
     */
    @Test
    void breaksTiesByIdWhenTimestampsAreEqual() {
        UUID roomId = UUID.randomUUID();
        Instant sameMoment = Instant.parse("2026-11-13T12:00:00.500000Z");
        List<ChatMessage> written = new ArrayList<>();
        for (int i = 0; i < 5; i = i + 1) {
            ChatMessage message = messageIn(roomId, sameMoment, "gleichzeitig " + i);
            insert(message);
            written.add(message);
        }
        List<ChatMessage> sorted = sortedById(written);
        ChatMessage anchor = sorted.get(0);

        List<ChatMessage> delivered = readEverythingAfter(roomId, anchor);

        assertEquals(4, delivered.size());
        assertNoDuplicates(delivered);
        for (int i = 0; i < delivered.size(); i = i + 1) {
            ChatMessage expected = sorted.get(i + 1);
            ChatMessage actual = delivered.get(i);
            assertEquals(expected, actual);
        }
    }

    /**
     * Der Weg durch die Datenbank verändert die Nachricht nicht.
     *
     * Darauf stützt sich die Zusage aus Spezifikation 2.3: Live und beim
     * Nachliefern entsteht dieselbe `data:`-Zeile. Verglichen wird beides —
     * der Record Feld für Feld und das JSON zeichenweise, mit EINEM
     * ObjectMapper für beide Seiten.
     *
     * Die Mikrosekunden sind der empfindliche Teil: Hätte sent_at mehr
     * Stellen als timestamptz speichern kann, stünden hier zwei
     * verschiedene Zeitangaben.
     */
    @Test
    void returnsExactlyWhatWasWritten() throws Exception {
        UUID roomId = UUID.randomUUID();
        Instant withMicroseconds = Instant.parse("2026-11-13T13:14:15.654321Z");
        ChatMessage anchor = messageIn(roomId, withMicroseconds.minusSeconds(1), "Anker");
        ChatMessage original = messageIn(roomId, withMicroseconds, "Hallo mit \"Anführungszeichen\"");
        insert(anchor);
        insert(original);

        List<ChatMessage> delivered = readEverythingAfter(roomId, anchor);

        assertEquals(1, delivered.size());
        ChatMessage readBack = delivered.get(0);
        assertEquals(original, readBack);
        ObjectMapper objectMapper = bootLikeObjectMapper();
        String liveJson = objectMapper.writeValueAsString(original);
        String deliveredJson = objectMapper.writeValueAsString(readBack);
        assertEquals(liveJson, deliveredJson);
        assertTrue(deliveredJson.contains("\"sentAt\":\"2026-11-13T13:14:15.654321Z\""), deliveredJson);
    }

    /** Blättert wie Schritt 7 es tun wird, bis keine Seite mehr kommt. */
    private List<ChatMessage> readEverythingAfter(UUID roomId, ChatMessage anchor) {
        List<ChatMessage> everything = new ArrayList<>();
        Instant anchorSentAt = anchor.sentAt();
        UUID anchorId = anchor.id();
        boolean morePages = true;
        while (morePages) {
            List<ChatMessage> page = repository.findPageAfter(roomId, anchorSentAt, anchorId);
            everything.addAll(page);
            if (page.size() < MessageHistoryRepository.PAGE_SIZE) {
                morePages = false;
            } else {
                ChatMessage lastOfPage = page.get(page.size() - 1);
                anchorSentAt = lastOfPage.sentAt();
                anchorId = lastOfPage.id();
            }
        }
        return everything;
    }

    private void insert(ChatMessage message) {
        Instant sentAt = message.sentAt();
        OffsetDateTime sentAtWithOffset = sentAt.atOffset(ZoneOffset.UTC);
        jdbcTemplate.update(INSERT_SQL,
                message.id(),
                message.roomId(),
                message.senderId(),
                message.senderName(),
                message.content(),
                sentAtWithOffset);
    }

    private ChatMessage messageIn(UUID roomId, Instant sentAt, String content) {
        UUID messageId = UUID.randomUUID();
        return new ChatMessage(messageId, roomId, "s1", "Test", content, sentAt);
    }

    private void assertNoDuplicates(List<ChatMessage> messages) {
        Set<UUID> seen = new HashSet<>();
        for (ChatMessage message : messages) {
            UUID id = message.id();
            boolean isNew = seen.add(id);
            assertTrue(isNew, "Nachricht " + id + " kam doppelt");
        }
    }

    private void assertAscending(List<ChatMessage> messages) {
        for (int i = 1; i < messages.size(); i = i + 1) {
            ChatMessage previous = messages.get(i - 1);
            ChatMessage current = messages.get(i);
            Instant previousSentAt = previous.sentAt();
            Instant currentSentAt = current.sentAt();
            boolean inOrder = !currentSentAt.isBefore(previousSentAt);
            assertTrue(inOrder, "Reihenfolge verletzt an Stelle " + i);
        }
    }

    /** Dieselbe Reihenfolge, die Postgres bei gleicher Zeitangabe liefert. */
    private List<ChatMessage> sortedById(List<ChatMessage> messages) {
        List<ChatMessage> sorted = new ArrayList<>(messages);
        sorted.sort((left, right) -> {
            String leftId = left.id().toString();
            String rightId = right.id().toString();
            return leftId.compareTo(rightId);
        });
        return sorted;
    }

    /**
     * Ein ObjectMapper, der Instant so schreibt wie der von Spring Boot.
     *
     * Boot schaltet WRITE_DATES_AS_TIMESTAMPS ab; ohne das stünde im JSON
     * eine Zahl statt der ISO-Zeichenkette. Für den Vergleich der beiden
     * Wege genügt EIN Mapper — welcher, ist gleichgültig, solange es
     * derselbe ist.
     */
    private ObjectMapper bootLikeObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        objectMapper.registerModule(javaTimeModule);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }
}
