# Live-Zustellung im chat-service — Umsetzungsplan

**Ziel:** Jede `chat-service`-Instanz stellt die Nachrichten aus `chat.delivery` über
Server-Sent Events zu und liefert beim Neuverbinden Verpasstes aus der Datenbank nach.

**Spezifikation:** [`spec-live-zustellung.md`](spec-live-zustellung.md) — dort stehen Vertrag,
Verhalten, Queues und Abnahmekriterien. Dieser Plan sagt nur, in welcher Reihenfolge gebaut
wird und mit welchem Auftrag.

**Vorbild für Aufbau:** [`plan-batch-writer.md`](plan-batch-writer.md).

**Tech-Stack:** Java 21, Spring Boot 3.5.16, Spring AMQP, Spring MVC (`SseEmitter`),
Spring JDBC (`JdbcTemplate`), PostgreSQL 16, RabbitMQ 3.13, JUnit 5, Testcontainers.

---

## Globale Vorgaben

Gelten für **jeden** Schritt:

- **Code auf Englisch**, **alles andere auf Deutsch** (`CLAUDE.md`).
- **Keine verschachtelten Aufrufe**, ein Ergebnis pro Zeile. Gilt auch in Tests.
- **Keine Streams**, `for`-Schleifen.
- **Kommentar über jeder Klasse und jeder Methode**, der das *Warum* erklärt.
- **Kein JPA**, `JdbcTemplate` wie im `batch-writer`.
- **Kein `ports:`-Eintrag** in `docker-compose.yml`.
- **Keine Geheimnisse im Repository**, nur `.env.example`.
- **Ein Schritt = ein Commit**, Commit-Message auf Deutsch.
- **Jeder Commit, den die KI schreibt, endet mit** (Auftrag, Abschnitt 01):
  ```
  Co-Authored-By: Claude Opus 5 <claude@pritz-it.com>
  ```
- **Erst testen, dann abhaken.** „Läuft“ erst, wenn es gestartet wurde.

---

## Reihenfolge und warum

Zuerst die **Broker-Seite** (Queue, Listener), dann **HTTP** (Stream), dann die
**Datenbankseite** (Nachliefern), zuletzt **Grenzen**, **Konfiguration** und **Abnahme**.

Grund: Der Normalfall braucht keine Datenbank (Spezifikation 3.4). Er steht nach Schritt 3
durchgängig und lässt sich von Hand beobachten, bevor der schwierigste Teil — das Nachliefern —
dazukommt. Fehlt dort etwas, weiss man, dass es nicht am Normalfall liegt.

Das Nachliefern ist in zwei Schritte geteilt (6 und 7): erst die Abfrage gegen eine echte
Datenbank, dann der Ablauf mit `pending` und `sentIds`. So hat jeder Schritt genau eine Sache,
die schiefgehen kann.

**Schon erledigt, vor diesem Plan:** `sentAt` auf Mikrosekunden kürzen (Commit `52a2ea3`). Das
war ein Fund aus der Spezifikationsarbeit, kein Bauschritt.

---

## Schritte

### Schritt 1 · Stream-Queue deklarieren

**Ziel:** Jede Instanz legt beim Start ihre Queue `chat.stream.<UUID>` an, mit allen
Eigenschaften aus Spezifikation 4.1, gebunden an `chat.delivery`. Log-Meldung
`Live queue … declared`. Die Werte stehen zunächst fest im Code; konfigurierbar werden sie in
Schritt 9.

**Warum an dieser Stelle:** Ohne Queue hat der Listener nichts zu konsumieren. Prüfbar ohne HTTP
und ohne Datenbank.

**Auftrag an die KI:**
> Leg beim Start die Stream-Queue nach Spezifikation 4.1 an: Name `chat.stream.<UUID>`, die
> UUID einmal pro Prozess in der `@Bean`-Methode erzeugt, kein Hostname. `durable = true`,
> nicht `exclusive`, nicht `auto-delete`, keine `AnonymousQueue`, keine DLQ. Argumente
> `x-expires 10000`, `x-max-length 950`, `x-overflow drop-head` — bei jeder Deklaration
> identisch. Bind sie an den Fanout `chat.delivery` und logge
> `Live queue chat.stream.<UUID> declared`. Schreib einen Testcontainers-Test gegen einen
> echten Broker, der Präfix, `durable`, alle drei Argumente und das Binding an
> `chat.delivery` prüft, und dass nach einer Neuverbindung derselbe Name verwendet wird.

**Test:** Testcontainers: Queue existiert, Name beginnt mit `chat.stream.`, `durable = true`,
`x-expires 10000`, `x-max-length 950`, `x-overflow drop-head`, Bindung an `chat.delivery`.
Nach einer Neuverbindung wird derselbe Name verwendet.

**Geprüft und korrigiert:** Beim Prüfen ist aufgefallen, dass die Queue nicht beim Start
entsteht, sondern erst beim ersten POST. Deswegen stimmt Spezifikation 4.1 erst ab
Schritt 2 und noch nicht für Schritt 1. Korrigiert wurde nichts.

```
14:34:39  Live queue chat.stream.8b9a10f5-41ec-42a1-ac0a-6587369d8e35 declared
14:35:07  Live queue chat.stream.1820a787-c827-4a2b-86fc-40a94183c475 declared
14:35:22  Live queue chat.stream.bcb97a79-9eed-4efe-9198-f1df3ebd0bdf declared
```
---

### Schritt 2 · Listener und Verbindungsregister

**Ziel:** `StreamConnection` (zunächst nur Emitter und `roomId`), Register
`Map<UUID, Set<StreamConnection>>`, Listener auf die Queue aus Schritt 1 mit Raumfilter,
Prefetch 50 und `concurrency = 1` (Spezifikation 3.1, 4.3). Der Listener wirft nie.

**Warum an dieser Stelle:** Die Hälfte des Normalfalls, noch ohne HTTP. Zustand
`CATCHING_UP`, `pending` und `sentIds` kommen bewusst erst in Schritt 7.

**Auftrag an die KI:**
> Bau den Listener auf die Stream-Queue aus Schritt 1 mit `concurrency = 1` und Prefetch 50
> (Spezifikation 3.1 und 4.3). Der Zieltyp `ChatMessage` kommt aus der Methodensignatur,
> `__TypeId__` wird nicht ausgewertet. Eine Nachricht geht nur an die `StreamConnection`s aus
> `Map[roomId]`, direkt per `emitter.send`; für einen Raum ohne Verbindung wird sie verworfen.
> Sie geht nicht an andere Räume, nicht in die Datenbank, nicht zurück an `chat.persist` oder
> den Exchange. Der Listener wirft nie, damit es kein Requeue gibt; keine DLQ. Das Register ist
> eine `ConcurrentHashMap` mit einem nebenläufigkeitsfesten `Set` pro Raum, weil
> Request-Thread, Listener-Thread, Ping-Job und ab Schritt 7 die Nachliefer-Threads darauf
> zugreifen. Zustände und `pending` kommen erst in Schritt 7. Test: Raum B bekommt nichts.

**Test:** Eine Nachricht für Raum A erreicht die Verbindung in Raum A, Raum B bekommt nichts.

**Geprüft und korrigiert:** `MessageStreamListenerTest`: 4 Tests grün. Die WARN-Zeile belegt den
Abriss-Pfad: Ein Fehler beim Senden trägt die Verbindung aus, und der Listener wirft nicht.
Im laufenden Stack mit zwei Instanzen zeigen beide Queues `consumers 1` und `messages_ready 0`.
Die Nachricht `3aa32f0f` wurde auf Instanz 1 publiziert und auf **beiden** Instanzen verworfen,
weil im Raum noch kein Stream offen ist. Damit ist belegt: Jede Instanz hat eine eigene Queue,
es gibt kein Competing Consumers. Korrigiert wurde nichts.

    16:16:51.354  WARN  Stream for room 3c85952e-… removed after a failed send: client aborted

    chat.stream.f0b7f459-3015-4abd-b7bf-4dada4e6226e   consumers 1   messages_ready 0
    chat.stream.1aeadf98-3885-4697-a336-d959f538f666   consumers 1   messages_ready 0

    15:19:31.830  chat-service-1  Message 3aa32f0f-… published to queue chat.persist and exchange chat.delivery
    15:19:31.833  chat-service-1  Message 3aa32f0f-… dropped, no open stream for room 39acf48b-…
    15:19:31.901  chat-service-2  Message 3aa32f0f-… dropped, no open stream for room 39acf48b-…

---

### Schritt 3 · SSE-Endpunkt

**Ziel:** `GET /stream?roomId=…` nach Spezifikation 2.3 und 3.7.2 Schritte 1–5: Prüfung der
`roomId`, `new SseEmitter(0L)`, Callbacks, Eintragen, `retry:2000`. Ereignisse mit `id:` und
einzeiligem `data:` aus dem `ObjectMapper` von Spring Boot. `Last-Event-ID` wird gelesen; ist
sie keine UUID, gilt sie als nicht vorhanden. Ausgewertet wird sie erst in Schritt 7.

**Warum an dieser Stelle:** Danach ist der Normalfall durchgängig. Ab hier kann man von Hand
zuschauen.

**Auftrag an die KI:**
> Bau `GET /stream?roomId=…` nach Spezifikation 2.3: `SseEmitter(0L)`, also ohne Timeout.
> Kopfzeilen `Content-Type: text/event-stream`, `Cache-Control: no-cache`, chunked, keine
> `Content-Length`. Die erste Zeile ist `retry:2000`, danach je Nachricht `id:<UUID>` und
> `data:<JSON einzeilig>`, ohne `event:`. Nimm dafür Springs `SseEmitter.event()`; die Zeilen
> tragen damit **kein** Leerzeichen nach dem Doppelpunkt, so wie in Spezifikation 2.3
> festgehalten. Weicht die erste gemessene Antwort davon ab, meldest du es, statt das Format
> von Hand nachzubauen. Serialisiere mit dem von Spring Boot bereitgestellten `ObjectMapper`
> zu einem `String` und sende den String, nicht das Objekt — nur so entsteht live dieselbe
> Zeichenkette wie beim Nachliefern. Die Callbacks `onCompletion`, `onTimeout` und `onError`
> tragen die Verbindung in jedem Fall aus der Map aus. `400`, wenn `roomId` fehlt oder keine
> UUID ist; eine unbekannte `roomId` gibt `200` mit stillem Stream. Eine `Last-Event-ID`, die
> keine UUID ist, wird wie keine behandelt, dazu WARN. Test: Integrationstest für das
> SSE-Format, dazu L4 und L5.

**Test:** Integrationstest für das SSE-Format (`retry:2000`, `id:` + einzeiliges `data:`),
dazu L4 und L5.

**Geprüft und korrigiert:** Gemessen im laufenden Stack an Instanz 1, Client und POST im selben
Container im Netz `chat-net`. Antwort `200` mit `Content-Type: text/event-stream`,
`Cache-Control: no-cache`, `Transfer-Encoding: chunked`, ohne `Content-Length` (L4). Die erste
Körperzeile ist `retry:2000`, danach je Nachricht `id:` und einzeiliges `data:`, ohne `event:`,
ohne Leerzeichen nach dem Doppelpunkt. Die Zeilen enden nur mit LF, `cat -A` zeigt kein `^M`.
`sentAt` hat 6 Nachkommastellen. Ohne `roomId` und mit `roomId=abc` kommt je `400` (L5).
Korrigiert wurde nichts.

    HTTP/1.1 200
    Cache-Control: no-cache
    Content-Type: text/event-stream
    Transfer-Encoding: chunked

    retry:2000$
    $
    id:a5d306bf-14ad-44e7-b509-75b582db8ebe$
    data:{"id":"a5d306bf-…","roomId":"39acf48b-…","senderId":"s1","senderName":"Test","content":"Schritt 3","sentAt":"2026-11-13T18:11:51.842417Z"}$
    $

    ohne roomId: 400
    roomId=abc:   400

---

### Schritt 4 · Ping und Herunterfahren

**Ziel:** `@Scheduled` schickt alle 15 s `:ping` an jede Verbindung (3.7.6). Bei
`ContextClosedEvent` werden alle Emitter mit `complete()` geschlossen (3.3).

**Warum an dieser Stelle:** Klein, und S4 hängt daran. Ohne das Schliessen wartet Graceful
Shutdown bis zu 30 s auf die offenen Streams.

**Auftrag an die KI:**
> Schick mit einem `@Scheduled`-Job alle 15 s `:ping` an jede Verbindung. Tote Clients werfen
> dabei `ClientAbortException`, `onError` trägt sie aus der Map aus (Spezifikation 3.7.6).
> Schliess beim `ContextClosedEvent` alle Emitter mit `complete()`, sonst wartet der graceful
> Shutdown bis zum SIGKILL (3.3). Danach verbinden sich die Browser mit `Last-Event-ID` neu,
> die AMQP-Verbindung schliesst, und die Queue ist nach 10 s weg. Der Ping läuft auf einem
> eigenen Scheduler, nicht auf dem Nachliefer-Pool aus Schritt 7: gibt es einen
> `TaskScheduler`-Bean, nimmt Spring ihn auch für `@Scheduled`, und dann stünde der Ping
> hinter bis zu 200 Nachlieferungen in der Warteschlange. Test: L6 für den Ping
> (`b.txt` enthält `:ping`), L7 für das Herunterfahren.

**Test:** L6 für den Ping (`b.txt` enthält `:ping`), L7 für das Herunterfahren.

**Geprüft und korrigiert:** `StreamPingTest` (4) und `StreamShutdownTest` (3): 7 Tests grün. Die
WARN-Zeile aus `StreamWriter` belegt, dass ein Ping an eine abgerissene Verbindung diese austrägt.
`StreamShutdown` schliesst beim Herunterfahren alle offenen Streams, auch wenn keiner offen ist
(2 / 0 / 2). Im laufenden Stack hielt ein Client in einem stillen Raum 35 s an Instanz 1: Er
bekam `retry:2000`, zwei `:ping` und kein `data:`. Das passt zu einem Intervall von 15 s, und
ein Ping trägt kein `id:`. Korrigiert wurde nichts.

    19:39:57.039  WARN  StreamWriter    Stream for room 0139d3c5-… removed after a failed send: client aborted
    19:39:57.088  INFO  StreamShutdown  Shutting down, closing 2 open streams
    19:39:57.095  INFO  StreamShutdown  Shutting down, closing 0 open streams
    19:39:57.101  INFO  StreamShutdown  Shutting down, closing 2 open streams

    retry:2000$
    $
    :ping$
    $
    :ping$
    $
    --- ping: 2  data: 0

---

### Schritt 5 · Datenbankzugang im chat-service

**Ziel:** JDBC und Postgres-Treiber im `pom.xml`, Datenquelle in `application.yml` mit
`initialization-fail-timeout: -1` (3.4), `POSTGRES_*` im Compose-Block des `chat-service`,
`depends_on` weiterhin nur auf `rabbitmq`.

**Warum an dieser Stelle:** Voraussetzung für Schritt 6, sonst nichts. Als eigener Schritt,
damit klar ist: Ab hier hat der `chat-service` eine Datenbank, und er muss trotzdem ohne sie
starten.

**Auftrag an die KI:**
> Gib dem `chat-service` Datenbankzugang nach Spezifikation 3.4: Datasource mit
> `POSTGRES_HOST/USER/PASSWORD/DB` (auch im Compose-Block des `chat-service`), JDBC-Treiber,
> `hikari.initialization-fail-timeout: -1`. Der Code liest nur, er schreibt nie. Kein
> `depends_on: postgres` mit `service_healthy`, kein Flyway, keine SQL-Skripte beim Start,
> keine Schemaänderung, kein Bean, der beim Start die Datenbank berührt. Der Start muss ohne
> Datenbank gelingen, und der Live-Weg berührt die Datenbank nie. Test: L8.

**Test:** L8 — `chat-service` startet neu, während Postgres gestoppt ist.

**Geprüft und korrigiert:** L8 im laufenden Stack. Zählerstand vorher: 5162. Postgres und
`batch-writer` um 18:54:25 gestoppt, `chat-service` um 18:54:27 neu gestartet:
`Started ChatServiceApplication` um 18:54:34, also ohne Datenbank. Bei weiterhin gestoppter
Datenbank empfing ein Client an Instanz 1 alle 100 gesendeten Nachrichten (`TAG=l8`). Nach dem
Start von Postgres und `batch-writer` stand der Zählerstand auf 5262, also +100. Der Live-Weg
braucht die Datenbank nicht, und der Schreibweg holt nach. Korrigiert wurde nichts.

    18:54:24  count(*) = 5162
    18:54:25  stop postgres batch-writer
    18:54:27  restart chat-service
    18:54:34  Started ChatServiceApplication in 4.208 seconds
              grep -c '^id:' l8.txt  →  100
              start postgres batch-writer
              count(*) = 5262

---

### Schritt 6 · Nachliefer-Abfrage

**Ziel:** Ein Repository mit zwei Methoden: Anker suchen (`sent_at` zu `id` und `roomId`) und
eine Seite nach dem Anker holen (Abfrage aus 3.7.3, `LIMIT 500`). Zeilen werden zum
`ChatMessage`-Record.

**Warum an dieser Stelle:** Die Abfrage lässt sich gegen eine echte Datenbank prüfen, ohne dass
SSE oder RabbitMQ im Spiel sind. Geht in Schritt 7 etwas schief, ist die Abfrage schon
ausgeschlossen.

**Hinweis zum Schema im Test:** Das Schema gehört dem `batch-writer`. Der Test liest dessen
`V1__create_message_table.sql` über den relativen Pfad und führt es aus, statt eine Kopie
anzulegen, die auseinanderlaufen könnte.

**Auftrag an die KI:**
> Bau im `chat-service` die zwei Abfragen nach Spezifikation 3.7.3: den Anker mit
> `SELECT sent_at FROM message WHERE id = ? AND room_id = ?` und das Nachliefern per Keyset
> mit `room_id = ?`, `sent_at >= ?`, `(sent_at, id) > (?, ?)`,
> `ORDER BY sent_at, id LIMIT 500`. Ein `RowMapper` baut daraus den vorhandenen
> `ChatMessage`-Record; `sent_at` wird als `Instant` gelesen
> (`getObject(…, OffsetDateTime.class)` oder `Timestamp.toInstant()`), nie als Zeichenkette
> formatiert — sonst steht im nachgelieferten JSON `2026-11-13 09:09:09.251491` statt
> `…T…Z`, und das ist genau die Abweichung, die Commit `52a2ea3` auf der Schreibseite
> verhindert hat. Test: Integrationstest mit Testcontainers-Postgres und
> dem Schema aus `V1__create_message_table.sql`; er prüft Blättern über mehr als 500 Zeilen
> und den Gleichstand bei `sent_at` und vergleicht das serialisierte JSON einer
> nachgelieferten Zeile zeichenweise mit dem der Live-Nachricht. Zusätzlich L12 mit
> `EXPLAIN ANALYZE` über `psql`.

**Test:** Testcontainers-Postgres mit dem Schema aus `V1__create_message_table.sql`: Blättern
über mehr als 500 Zeilen, Gleichstand bei `sent_at`. Zusätzlich L12.

**Geprüft und korrigiert:** `MessageHistoryRepositoryIntegrationTest`: 5 Tests grün, gegen
Testcontainers `postgres:16` mit dem Schema aus `V1__create_message_table.sql` —
`pagesThroughMoreRowsThanOnePageHolds`, `breaksTiesByIdWhenTimestampsAreEqual`,
`findsTheTimestampOfTheAnchor`, `doesNotFindAnAnchorFromAnotherRoom`,
`returnsExactlyWhatWasWritten`. L12 im laufenden Stack: 300 Nachrichten in einem zweiten Raum
neben 5262 im ersten. Der Planer wählt `Index Scan Backward using idx_message_room_sent_at` mit
`Incremental Sort`, liefert 299 Zeilen (der Anker selbst ist ausgeschlossen) und braucht
0,197 ms, Limit 10 ms. Korrigiert wurde nichts.

    Limit  (actual rows=299)
      ->  Incremental Sort  Sort Key: sent_at, id  Presorted Key: sent_at
            ->  Index Scan Backward using idx_message_room_sent_at on message
                  Index Cond: (room_id = '7f3c2a10-…') AND (sent_at >= '…19:06:58.076961+00')
                  Filter: ROW(sent_at, id) > ROW('…19:06:58.076961+00', 'f65742e9-…')
                  Rows Removed by Filter: 1
    Planning Time: 0.213 ms
    Execution Time: 0.197 ms

---

### Schritt 7 · Nachliefer-Ablauf

**Ziel:** Spezifikation 3.7.2 Schritte 6–12: Zustand `CATCHING_UP`/`LIVE`, `pending`,
`sentIds` mit der 5-Sekunden-Grenze, Sperre pro Verbindung, `ThreadPoolTaskScheduler` mit
4 Threads und 1 s Verzögerung, Anker-Wiederholungen, Blättern, Umschalten.

**Warum an dieser Stelle:** Der grösste Schritt. Braucht Register (2), Stream (3) und Abfrage (6).

**Auftrag an die KI:**
> Bau den Ablauf für einen Client mit `Last-Event-ID` nach Spezifikation 3.7.2:
> (1) Verbindung eintragen, vor jeder Datenbankabfrage; `registeredAt` merken, Zustand
> `CATCHING_UP`. (2) `retry:2000` senden. (3) Die Nachlieferung mit 1 s Verzögerung
> einplanen (`ThreadPoolTaskScheduler`, 4 Threads). (4) Anker suchen, bis zu 3 Wiederholungen
> im Abstand von 500 ms; nicht gefunden oder Datenbank weg: WARN, direkt umschalten.
> (5) Seiten à 500 senden; IDs mit `sent_at ≥ registeredAt − 5 s` kommen in `sentIds`, dann
> weiterblättern. (6) Unter der Sperre umschalten: `pending` ohne die IDs aus `sentIds`
> senden, Zustand `LIVE`, `sentIds` verwerfen. Der Listener hängt bei `CATCHING_UP` unter der
> Sperre an `pending` an. Die Grenzen kommen erst in Schritt 8. Test: Integrationstest
> „Nachliefern ohne Duplikat“, ein Testfall mit raumfremder ID (nach den Wiederholungen wie
> ohne Kopfzeile), dazu L10.

**Test:** Integrationstest „Nachliefern ohne Duplikat“, Testfall mit raumfremder ID, dazu L10.

**Geprüft und korrigiert:** `StreamDeliveryTest`: 7 Tests grün —
`sendsPendingMessagesAfterTheCatchUp`, `sendsImmediatelyOnceLive`,
`doesNotSendAMessageTwiceWhenItIsBothPendingAndInTheDatabase`,
`remembersOnlyRecentIdsForDeduplication`, `goesLiveWhenTheDatabaseIsNotReachable`,
`pagesUntilAPageIsNotFull`, `goesLiveWhenTheAnchorIsNotFound`. Die Tests laufen mit Mocks, nicht
gegen eine echte Datenbank. Den Fall „pending und DB überschneiden sich“ belegt im echten Stack
L10.

L10 mit zwei Instanzen: 300 Nachrichten an Raum A, nach jeder 50. eine Pause von 1 s. Client a
an Instanz 1 bricht nach 8 s ab, Client b verbindet sich 1 s später mit `Last-Event-ID` an
Instanz 2. Ein Referenzclient bleibt die ganze Zeit an Instanz 1. Ergebnis: a 150 + b 150 = 300
IDs, 0 Duplikate, 0 Reihenfolgefehler, Referenz 300, das `data:` aus dem Nachliefern ist
zeichengleich mit dem live empfangenen. Instanz 2 meldet `Catch-up … delivered 100 messages`
und `0 buffered messages sent, 50 skipped as duplicates`. Ping und Nachliefern laufen auf
getrennten Threads (`stream-ping-*`, `stream-catchup-*`).

Korrigiert wurde nichts am Code, nur am Messverfahren. Clients, die per `docker run … > datei`
unter Git Bash schreiben, hörten mitten im Lauf auf zu lesen. Der Dienst blieb dann 60 s im
Schreiben hängen (`SocketTimeoutException`), und am Ende fehlten 14–15 Nachrichten. Gemessen wird
deshalb mit allen Clients in einem Container, der in Dateien im Container schreibt.

    LAST=86ae8dde-769e-409a-a4d2-31b091607c10
    IDs a: 150  b: 150  zusammen: 300
    Duplikate: 0
    Reihenfolgefehler: 0
    Referenz: 300
    data-Abweichungen b gegen live: 0
    19:19:43.611  chat-service-2  Catch-up for room 39acf48b-… delivered 100 messages
    19:19:43.611  chat-service-2  Room 39acf48b-… switched to live, 0 buffered messages sent, 50 skipped as duplicates

---

### Schritt 8 · Grenzen

**Ziel:** `pending` höchstens 1000, höchstens 200 gleichzeitige Nachlieferungen. Beim Erreichen
jeweils Emitter schliessen und WARN (3.7.7).

**Warum an dieser Stelle:** Erst begrenzen, wenn das Verhalten steht. Sonst testet man die
Grenze und den Ablauf gleichzeitig.

**Auftrag an die KI:**
> Bau die Grenzen nach Spezifikation 3.7.7: `pending` pro Verbindung höchstens 1000; beim
> Erreichen wird der Emitter mit `complete()` geschlossen, mit WARN, und der Browser verbindet
> sich nach 2 s ab der letzten erhaltenen ID neu. Höchstens 200 gleichzeitige
> Nachlieferungen; darüber wird der Emitter geschlossen, der Browser versucht es erneut. Die
> Grenze der Stream-Queue (950 „ready“ + 50 Prefetch, `drop-head` verwirft still die älteste)
> steht schon seit Schritt 1. Test: Unit-Test „pending am Limit“; L9 nur als Messung.

**Test:** Unit-Test „pending am Limit“: die 1001. Nachricht schliesst den Emitter. L9 nur als
Messung.

**Geprüft und korrigiert:** `StreamLimitsTest`: 2 Tests grün. Ist `pending` voll, wird der
Emitter geschlossen (`buffer full during catch-up (1000 messages)`). Laufen 200 Nachlieferungen,
wird jede weitere abgewiesen (`too many catch-ups running (200)`).

L9 als Messung: Client an Instanz 1, Instanz 1 per `docker pause` angehalten, 5000 Nachrichten
über Instanz 2 (19:26:12 bis 19:26:43), dann `unpause` und 10 Nachrichten `nachher`. Der
Höchstwert von `messages_ready + messages_unacknowledged` in `s6.log` ist 1000. Die Queue von
Instanz 1 (`chat.stream.1cd94c57-…`) besteht nach dem Fortsetzen weiter. Der Client erhielt
1000 der 5000: die ersten 50 (unbestätigt im Socket, Prefetch) und die letzten 950 (ready, ältere
per `drop-head` verworfen), danach alle 10 `nachher` innerhalb von 30 s. Das entspricht
Spezifikation 3.5. Die Lücke 51–4050 fehlt dem Client still, wie in offenem Punkt 2 beschrieben.
Korrigiert wurde nichts am Code. Im Messskript ist `docker pause` statt `docker compose pause`
nötig, weil das alle Instanzen anhält, und unter Git Bash `MSYS_NO_PATHCONV=1` vor `docker exec`.

    Hoechstwert aller chat.stream.*: 1000
    Queue von Instanz 1 noch da: 1
    Client vorher: 1000  nachher: 10
    1-50: 50  Mitte: 0  4051-5000: 950

---

### Schritt 9 · Konfiguration und Doku

**Ziel:** Heartbeat an drei Stellen (3.6), `STREAM_*` in `.env.example` und im Compose-Block
(4.2), veraltete Kommentare in `RabbitConfig.java` und `QueueNames.java`, Hinweis in
`PLANUNG.md` 3.4 (4.4).

**Warum an dieser Stelle:** Betrifft den ganzen Stack, auch den `batch-writer`. Zum Schluss,
damit vorher kein Schritt von einer geänderten Broker-Einstellung abhängt.

**Auftrag an die KI:**
> Zieh die Konfiguration nach Spezifikation 3.6, 4.2 und 4.4 nach: im Compose-Block
> `chat-service` `STREAM_*` ergänzen, `depends_on` nur auf `rabbitmq`, kein `ports:`; im Block
> `rabbitmq` `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: "-rabbit heartbeat 300"`; in
> `chat-service/application.yml` `requested-heartbeat: 300s` und Standardwerte für `STREAM_*`;
> in `batch-writer/application.yml` `requested-heartbeat: 60s`; in `.env.example`
> `STREAM_QUEUE_MAX_LENGTH=950`, `STREAM_PREFETCH=50`, `STREAM_QUEUE_EXPIRES_MS=10000`. Diese
> drei Werte ersetzen die Konstanten aus Schritt 1: `RabbitConfig` liest sie per `@Value` mit
> den Vorgaben aus `application.yml`. Ohne diese Zeile hätte `.env` keine Wirkung, und L3
> würde drei festverdrahtete Zahlen messen. Zieh
> die Kommentare „web-gateway/WebSocket“ in `RabbitConfig.java:41-44` und `QueueNames.java:15`
> nach und ergänz in `PLANUNG.md` 3.4 einen Hinweis auf die Abweichung. Test: L14 und L11.

**Test:** L14 (`chat-service` 300, `batch-writer` 60) und L11.

**Geprüft und korrigiert:** L14: `rabbitmqctl list_connections peer_host timeout`, Adressen über
`docker network inspect chat-net` zugeordnet: `chat-service-1` 300, `chat-service-2` 300,
`batch-writer` 60. Die Broker-Einstellung `-rabbit heartbeat 300` greift also (offener Punkt 1
der Spezifikation erledigt), und der Schreibweg bleibt bei 60. L11: keine Warnung zu
transienten Queues im Broker-Log. L3 nachgeprüft: zwei Queues, je `durable`, `consumers 1`,
`x-expires 10000`, `x-max-length 950`, `x-overflow drop-head`. Gegenprobe zur Konfiguration: Mit
`STREAM_QUEUE_MAX_LENGTH=900` entsteht eine Queue mit `x-max-length 900`. Der Wert kommt also
aus der Umgebung, nicht aus dem Code. Nach dem Zurücksetzen wieder 950. Der Hinweis auf die
Abweichung steht in `PLANUNG.md` 3.4. Die verbleibenden Erwähnungen von „web-gateway“ in
`RabbitConfig.java` und `MessageController.java` sind korrekt: Sie beschreiben die Abweichung
und den REST-Weg. `mvn -pl chat-service test`: 53 Tests grün. Korrigiert wurde nichts.

    172.18.0.2  300   it3c-m321-chat-service-1
    172.18.0.6  300   it3c-m321-chat-service-2
    172.18.0.5   60   it3c-m321-batch-writer-1

    chat.stream.0315a2b9-…  consumers 1  [x-expires 10000, x-overflow drop-head, x-max-length 950]
    chat.stream.7852c452-…  consumers 1  [x-expires 10000, x-overflow drop-head, x-max-length 950]

    STREAM_QUEUE_MAX_LENGTH=900  →  chat.stream.7bee6c95-…  x-max-length 900

Korrigiert: veralteter Kommentar in `ChatMessage.java`, der das `web-gateway` noch als
Verbraucher von `chat.delivery` nannte.

---

### Schritt 10 · Abnahme

**Ziel:** L1 bis L14 aus Spezifikation 5 im laufenden Stack ausführen und die Messwerte
eintragen.

**Warum an dieser Stelle:** Erst jetzt ist alles da. Was hier nicht stimmt, wird als eigener
Commit korrigiert, nicht stillschweigend.

**Auftrag an die KI:**
> Führ alle Kriterien L1 bis L14 aus Spezifikation 5 aus, vor allem die noch offenen: L7
> (Queue nach S4 weg, fünfmal, ≤ 30 s), L9 (Höchstwert in `s6.log` ≤ 1000), L12
> (Abfrageplan mit Daten in mehreren Räumen) und L14 (Heartbeat `300` für `chat-service`,
> `60` für `batch-writer`). Trag die Ergebnisse in die Tabelle „Bereits gemessen“ in
> Abschnitt 5 ein, ergänz in 3.3 den Wert aus L7, und schliess oder aktualisier in
> Abschnitt 6 die offenen Punkte 1 (Heartbeat) und 8 (Unschärfe bei `x-expires`).

**Test:** Die Tabelle selbst.

**Geprüft und korrigiert:** Die Tabelle in Abschnitt 5 wurde am 13.11.2026 zwischen 19:00
und 21:00 gefüllt (Commit `0ee63f3`). Gegen 21:45 habe ich die Kriterien, die sich ohne
Umbau wiederholen lassen, ein zweites Mal selbst ausgeführt, mit zwei Instanzen:

| Nr | Zweiter Lauf | Gleich wie in der Tabelle? |
|---|---|---|
| L1 | `mvn clean test`: `BUILD SUCCESS`, 53 + 11 = 64 Tests, 0 Fehler | ja |
| L2 | 11 × `PublishedPort: 0` | ja |
| L3 | 2 Queues, je `true` / `1` / drei Argumente, 2 Bindungen an `chat.delivery` | ja |
| L6 | `a1` und `a2` je 400 IDs, 0 Duplikate, je Absender 200 Nachrichten und 0 Reihenfolgefehler, `diff` 0; Raum B 0 × `data:`, 5 × `:ping` | ja (vorher 4 Pings, hängt nur von der Laufzeit ab) |
| L7 | 12, 13, 11, 13, 13 s | ja, alle ≤ 30 s |
| L11 | 0 Treffer | ja |
| L12 | kleiner Raum (300 Zeilen): `Bitmap Index Scan on idx_message_room_sent_at`, 0,283 ms | ja |
| L13 | 0 Treffer | ja |
| L14 | beide `chat-service` 300, `batch-writer` 60 | ja |

Nicht wiederholt, die Werte in der Tabelle stammen aus dem ersten Lauf: L4, L5, L8, L9, L10
und L12 für den grossen Raum.

Zwei Schwächen der Befehle, die das Ergebnis nicht ändern:
- **L6, `diff`:** Im `curl`-Container läuft BusyBox-`sh`, und die Fehlermeldung von `diff`
  landet nicht in `wc -l`. Die `0` allein beweist also wenig. Die anderen Zahlen tragen den
  Schluss trotzdem: Beide Clients haben je 400 verschiedene IDs und alle 400 Inhalte gesehen.
  **Neu gemessen** ohne Prozess-Substitution: `diff` Exit-Code 0, keine Fehlerausgabe, und
  `comm` zeigt 400 gemeinsame IDs, 0 nur in `a1`, 0 nur in `a2` (Spezifikation Abschnitt 5).
- **L7:** Zwischen zwei Zählungen steht kein `sleep`. Der Takt ist also so lang, wie ein
  `rabbitmqctl`-Aufruf dauert (etwa eine Sekunde). Die Neuverbindung des Clients wurde in
  diesem Lauf nicht geprüft, weil seine Ausgabe verworfen wird.

Die vielen `ERROR`-Zeilen im Log von L1 sind kein Fehler. Sie kommen aus den Testkontexten,
die Spring zwischen den Testklassen zwischenspeichert. Diese Kontexte versuchen weiter, ihren
inzwischen gestoppten Testcontainers-Broker zu erreichen. „auto-delete queue(s)“ ist dabei
nur Springs allgemeiner Meldungstext. Korrigiert wurde nichts.
