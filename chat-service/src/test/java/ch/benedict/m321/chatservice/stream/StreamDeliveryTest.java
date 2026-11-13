package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.repository.MessageHistoryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Prüft den Nachliefer-Ablauf aus Spezifikation 3.7.2 und die Zusage aus
 * 3.7.5: nichts verloren, nichts doppelt.
 *
 * Geprüft wird der Zustandsautomat, nicht die Datenbank — die Abfrage selbst
 * hat ihren eigenen Test gegen eine echte Postgres. Hier ist das Repository
 * deshalb eine Attrappe, und der Test kann genau die Überschneidung
 * herstellen, auf die es ankommt: eine Nachricht, die gleichzeitig live
 * eintrifft UND auf einer nachgelieferten Seite steht.
 *
 * catchUp wird direkt aufgerufen statt über scheduleCatchUp. Die Sekunde
 * Verzögerung ist Terminierung, keine Logik; sie im Test abzuwarten würde
 * ihn nur langsam machen.
 */
class StreamDeliveryTest {

    private final MessageHistoryRepository repository = mock(MessageHistoryRepository.class);

    private final StreamRegistry streamRegistry = new StreamRegistry();

    private final StreamWriter streamWriter = new StreamWriter(objectMapperWithInstantSupport(), streamRegistry);

    private final StreamDelivery streamDelivery = new StreamDelivery(streamWriter, repository, idleScheduler());

    /**
     * Der Kern von Szenario S7: Die Überschneidung erzeugt kein Duplikat.
     *
     * Aufbau: Der Client verbindet sich mit einem Anker neu. Während die
     * Nachlieferung noch läuft, trifft Nachricht B live ein — und B steht
     * auch auf der Seite aus der Datenbank, weil der batch-writer sie
     * inzwischen geschrieben hat. Der Client darf B genau EINMAL sehen.
     */
    @Test
    void doesNotSendAMessageTwiceWhenItIsBothPendingAndInTheDatabase() throws Exception {
        UUID roomId = UUID.randomUUID();
        Instant registeredAt = Instant.parse("2026-11-13T10:00:10Z");
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = catchingUpConnection(emitter, roomId, registeredAt);
        ChatMessage anchor = messageIn(roomId, Instant.parse("2026-11-13T10:00:00Z"), "Anker");
        ChatMessage missed = messageIn(roomId, Instant.parse("2026-11-13T10:00:05Z"), "verpasst");
        ChatMessage overlapping = messageIn(roomId, Instant.parse("2026-11-13T10:00:11Z"), "ueberschneidend");
        givenAnchor(anchor, roomId);
        givenOnePage(roomId, anchor, List.of(missed, overlapping));

        // Trifft live ein, waehrend die Verbindung noch CATCHING_UP ist.
        streamDelivery.deliverLive(connection, overlapping);
        streamDelivery.catchUp(connection, anchor.id());

        List<ChatMessage> sent = messagesSentTo(emitter);
        assertEquals(2, sent.size(), "Gesendet wurde: " + sent);
        assertEquals(missed, sent.get(0));
        assertEquals(overlapping, sent.get(1));
    }

    /**
     * Was nach der Registrierung eintrifft und NICHT in der Datenbank
     * steht, kommt aus `pending` — nach dem Nachgelieferten.
     */
    @Test
    void sendsPendingMessagesAfterTheCatchUp() throws Exception {
        UUID roomId = UUID.randomUUID();
        Instant registeredAt = Instant.parse("2026-11-13T10:00:10Z");
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = catchingUpConnection(emitter, roomId, registeredAt);
        ChatMessage anchor = messageIn(roomId, Instant.parse("2026-11-13T10:00:00Z"), "Anker");
        ChatMessage missed = messageIn(roomId, Instant.parse("2026-11-13T10:00:05Z"), "verpasst");
        ChatMessage brandNew = messageIn(roomId, Instant.parse("2026-11-13T10:00:12Z"), "ganz neu");
        givenAnchor(anchor, roomId);
        givenOnePage(roomId, anchor, List.of(missed));

        streamDelivery.deliverLive(connection, brandNew);
        streamDelivery.catchUp(connection, anchor.id());

        List<ChatMessage> sent = messagesSentTo(emitter);
        assertEquals(2, sent.size());
        assertEquals(missed, sent.get(0), "Das Nachgelieferte muss zuerst kommen");
        assertEquals(brandNew, sent.get(1));
    }

    /** Nach dem Umschalten geht alles sofort hinaus. */
    @Test
    void sendsImmediatelyOnceLive() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = new StreamConnection(emitter, roomId);
        ChatMessage message = messageIn(roomId, Instant.now(), "live");

        streamDelivery.deliverLive(connection, message);

        List<ChatMessage> sent = messagesSentTo(emitter);
        assertEquals(1, sent.size());
        assertEquals(message, sent.get(0));
    }

    /**
     * Eine Last-Event-ID aus einem fremden Raum: nach den Wiederholungen
     * wird live geschaltet, als wäre keine Kopfzeile gekommen.
     *
     * Wichtig ist das Ergebnis, nicht die Lücke: Ein Client, der in
     * CATCHING_UP steckenbliebe, sähe nie wieder eine Nachricht.
     */
    @Test
    void goesLiveWhenTheAnchorIsNotFound() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = catchingUpConnection(emitter, roomId, Instant.now());
        UUID unknownAnchor = UUID.randomUUID();
        when(repository.findSentAt(eq(unknownAnchor), eq(roomId))).thenReturn(Optional.empty());
        ChatMessage waiting = messageIn(roomId, Instant.now(), "wartet");

        streamDelivery.deliverLive(connection, waiting);
        streamDelivery.catchUp(connection, unknownAnchor);

        List<ChatMessage> sent = messagesSentTo(emitter);
        assertEquals(1, sent.size(), "Das Gepufferte muss trotzdem hinausgehen");
        assertEquals(waiting, sent.get(0));
        synchronized (connection.lock()) {
            assertTrue(connection.isLive());
        }
    }

    /**
     * Eine nicht erreichbare Datenbank darf den Client nicht einfrieren.
     *
     * Der Stream läuft live weiter, die Lücke steht im Log — offener Punkt
     * 5 der Spezifikation.
     */
    @Test
    void goesLiveWhenTheDatabaseIsNotReachable() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = catchingUpConnection(emitter, roomId, Instant.now());
        UUID anchorId = UUID.randomUUID();
        RuntimeException databaseGone = new RuntimeException("connection refused");
        when(repository.findSentAt(eq(anchorId), eq(roomId))).thenThrow(databaseGone);

        streamDelivery.catchUp(connection, anchorId);

        synchronized (connection.lock()) {
            assertTrue(connection.isLive());
        }
    }

    /**
     * Blättern: Eine volle Seite führt zu einer zweiten Abfrage mit der
     * letzten Zeile als neuem Anker.
     */
    @Test
    void pagesUntilAPageIsNotFull() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = catchingUpConnection(emitter, roomId, Instant.now());
        ChatMessage anchor = messageIn(roomId, Instant.parse("2026-11-13T09:00:00Z"), "Anker");
        List<ChatMessage> fullPage = fullPageIn(roomId);
        ChatMessage lastOfFullPage = fullPage.get(fullPage.size() - 1);
        ChatMessage onSecondPage = messageIn(roomId, Instant.parse("2026-11-13T09:30:00Z"), "zweite Seite");
        givenAnchor(anchor, roomId);
        givenOnePage(roomId, anchor, fullPage);
        givenOnePage(roomId, lastOfFullPage, List.of(onSecondPage));

        streamDelivery.catchUp(connection, anchor.id());

        List<ChatMessage> sent = messagesSentTo(emitter);
        int expected = MessageHistoryRepository.PAGE_SIZE + 1;
        assertEquals(expected, sent.size());
        ChatMessage lastSent = sent.get(sent.size() - 1);
        assertEquals(onSecondPage, lastSent);
    }

    /**
     * Alte Nachrichten landen nicht in `sentIds`.
     *
     * Sonst wüchse das Set bei einem Client, der eine Stunde weg war, auf
     * alles, was er verpasst hat. Doppelt sein können nur Nachrichten aus
     * den fünf Sekunden vor der Registrierung (Spezifikation 3.7.5).
     */
    @Test
    void remembersOnlyRecentIdsForDeduplication() {
        UUID roomId = UUID.randomUUID();
        Instant registeredAt = Instant.parse("2026-11-13T10:00:00Z");
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = catchingUpConnection(emitter, roomId, registeredAt);
        ChatMessage longAgo = messageIn(roomId, registeredAt.minusSeconds(60), "lange her");
        ChatMessage justBefore = messageIn(roomId, registeredAt.minusSeconds(2), "kurz vorher");

        connection.rememberSentId(longAgo);
        connection.rememberSentId(justBefore);

        assertFalse(connection.wasSentDuringCatchUp(longAgo));
        assertTrue(connection.wasSentDuringCatchUp(justBefore));
    }

    private StreamConnection catchingUpConnection(SseEmitter emitter, UUID roomId, Instant registeredAt) {
        return new StreamConnection(emitter, roomId, registeredAt, StreamState.CATCHING_UP);
    }

    private void givenAnchor(ChatMessage anchor, UUID roomId) {
        Instant sentAt = anchor.sentAt();
        Optional<Instant> found = Optional.of(sentAt);
        when(repository.findSentAt(eq(anchor.id()), eq(roomId))).thenReturn(found);
    }

    private void givenOnePage(UUID roomId, ChatMessage after, List<ChatMessage> page) {
        Instant sentAt = after.sentAt();
        UUID id = after.id();
        when(repository.findPageAfter(eq(roomId), eq(sentAt), eq(id))).thenReturn(page);
    }

    /** Eine volle Seite, damit das Blättern weitergeht. */
    private List<ChatMessage> fullPageIn(UUID roomId) {
        List<ChatMessage> page = new ArrayList<>();
        Instant start = Instant.parse("2026-11-13T09:10:00Z");
        for (int i = 0; i < MessageHistoryRepository.PAGE_SIZE; i = i + 1) {
            Instant sentAt = start.plusMillis(i);
            ChatMessage message = messageIn(roomId, sentAt, "Seite 1 Nummer " + i);
            page.add(message);
        }
        return page;
    }

    /**
     * Die Nachrichten, die beim Emitter angekommen sind, in der Reihenfolge
     * des Sendens.
     *
     * Gelesen wird aus dem fertigen SSE-Ereignis, nicht aus einem
     * Zwischenobjekt — so prüft der Test, was wirklich hinausgegangen wäre.
     */
    private List<ChatMessage> messagesSentTo(SseEmitter emitter) throws Exception {
        ArgumentCaptor<SseEmitter.SseEventBuilder> captor = ArgumentCaptor.forClass(SseEmitter.SseEventBuilder.class);
        verify(emitter, org.mockito.Mockito.atLeast(0)).send(captor.capture());
        List<SseEmitter.SseEventBuilder> events = captor.getAllValues();
        List<ChatMessage> messages = new ArrayList<>();
        ObjectMapper objectMapper = objectMapperWithInstantSupport();
        for (SseEmitter.SseEventBuilder event : events) {
            String text = StreamTestSupport.textOf(event);
            String json = StreamTestSupport.dataJsonOf(text);
            if (json == null) {
                continue;
            }
            ChatMessage message = objectMapper.readValue(json, ChatMessage.class);
            messages.add(message);
        }
        return messages;
    }

    private ChatMessage messageIn(UUID roomId, Instant sentAt, String content) {
        UUID messageId = UUID.randomUUID();
        return new ChatMessage(messageId, roomId, "s1", "Test", content, sentAt);
    }

    /**
     * Ein Scheduler, der nichts tut.
     *
     * Dieser Test ruft catchUp direkt auf, der Scheduler wird also nie
     * gebraucht — gebraucht wird nur ein Objekt für den Konstruktor.
     */
    private static ThreadPoolTaskScheduler idleScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.initialize();
        return scheduler;
    }

    private static ObjectMapper objectMapperWithInstantSupport() {
        ObjectMapper objectMapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        objectMapper.registerModule(javaTimeModule);
        return objectMapper;
    }
}
