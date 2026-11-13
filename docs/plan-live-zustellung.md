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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_

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

**Geprüft und korrigiert:** _(nach dem Schritt ausfüllen)_
