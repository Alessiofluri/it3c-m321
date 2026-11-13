# Spezifikation: Live-Zustellung im chat-service

**Modul M321 · Klasse IT3c · Bewertung 2 · 13.11.2026**

Der `chat-service` bekommt einen zweiten Weg nach aussen: Jede Nachricht, die am
Fanout-Exchange `chat.delivery` ankommt, stellt er über Server-Sent Events
(`GET /stream?roomId=…`) an die Clients des betroffenen Raums zu. Reisst eine Verbindung ab,
liefert er beim Neuverbinden die verpassten Nachrichten aus der Datenbank nach.

Grundlage: [`../PLANUNG.md`](../PLANUNG.md) Abschnitt 3.4,
[`spec-batch-writer.md`](spec-batch-writer.md) und der Auftrag zu Bewertung 2.

---

## 1. Zweck und Abgrenzung

### Was der Dienst neu tut

Jede `chat-service`-Instanz hängt mit einer **eigenen** Queue am Fanout. Sie konsumiert jede
Nachricht, filtert nach `roomId` und schreibt sie in die offenen SSE-Streams dieses Raums.
Für das Nachliefern liest sie die Tabelle `message`.

**Warum im `chat-service`:** Er kennt den Vertrag der Nachricht bereits, denn er erzeugt sie.
Ein eigener Dienst wäre ein weiterer Container mit einer weiteren Kopie desselben Records.

**Abweichung von `PLANUNG.md`:** Dort (Zeilen 160 und 200) ist das `web-gateway` der
Verbraucher von `chat.delivery`. Mit dieser Spezifikation übernimmt das der `chat-service`.

### Was der Dienst bewusst nicht tut

| Nicht enthalten | Warum |
|---|---|
| Keycloak, Token-Prüfung | Laut Auftrag nicht Teil von Bewertung 2. Der Dienst ist von aussen nicht erreichbar. |
| `web-gateway`, React, JavaFX | Laut Auftrag keine Punkte. Das Gateway muss später `Last-Event-ID` durchreichen und darf SSE nicht puffern (Abschnitt 6). |
| Räume anlegen oder prüfen | Nicht im Auftrag. Eine unbekannte `roomId` ergibt deshalb einen stillen Stream statt `404`. |
| Historie („die letzten 50“) | Gehört zum Lesepfad. Der Stream liefert nur nach, was ein Client *verpasst* hat. |
| Änderungen am Schema | Das Schema gehört dem `batch-writer` (Abschnitt 3.7.4). |
| Publisher Confirms | Bleibt offener Punkt aus Bewertung 1. |
| Exactly-once in jedem Grenzfall | Wird nicht behauptet. Die Restlücken stehen in Abschnitt 6. |

---

## 2. Vertrag

### 2.1 Woher ich das weiss

| Frage | Beleg |
|---|---|
| Name des Exchange | `chat-service/.../config/QueueNames.java:16` → `DELIVERY_EXCHANGE = "chat.delivery"` |
| Typ und Eigenschaften | `chat-service/.../config/RabbitConfig.java:47-48` → `new FanoutExchange(…, true, false)`: durable, nicht auto-delete |
| Wie gesendet wird | `chat-service/.../service/MessagePublisher.java:36` → `convertAndSend(DELIVERY_EXCHANGE, "", message)`, leerer Routing-Key (Zeile 22). Zeile 35 sendet *vorher* nach `chat.persist`. |
| Felder | `chat-service/.../dto/ChatMessage.java:14-20` |
| Wer `id` und `sentAt` vergibt | `chat-service/.../service/MessageService.java:39-46` — der Server |
| JSON-Format | `RabbitConfig.java:59-61` → `Jackson2JsonMessageConverter` mit dem `ObjectMapper` von Spring Boot |
| Kopfzeilen | **Gemessen am 13.11.2026:** Testqueue an `chat.delivery` gebunden, Nachricht über `POST /messages` gesendet, mit `rabbitmqadmin get` abgeholt. Testqueue danach gelöscht. |
| Spalten für das Nachliefern | `batch-writer/.../db/migration/V1__create_message_table.sql`, Index in Zeile 21 |

### 2.2 Eingang: was am Exchange ankommt

```json
{"id":"3bf82e40-0d89-4e6a-a6ce-7cce4b0f1bb5","roomId":"39acf48b-5d49-40f0-a5d6-cd5bfdb59385","senderId":"s1","senderName":"Test","content":"Hallo","sentAt":"2026-11-13T09:09:09.251491Z"}
```

| Feld | JSON-Typ | Bedeutung |
|---|---|---|
| `id` | String (UUID) | vom `chat-service` vergeben |
| `roomId` | String (UUID) | danach wird gefiltert |
| `senderId` | String | `sub` aus Keycloak, später |
| `senderName` | String | Anzeigename |
| `content` | String | Text |
| `sentAt` | String (ISO-8601, UTC, `Z`) | **höchstens 6 Nachkommastellen** (Mikrosekunden), in `MessageService` gekürzt, passend zu `timestamptz` |

Gemessene Kopfzeilen:

| Kopfzeile | Wert | Auswerten? |
|---|---|---|
| `content_type` | `application/json` | **Ja — als einzige.** |
| `content_encoding` | `UTF-8` | nein |
| `__TypeId__` | `ch.benedict.m321.chatservice.dto.ChatMessage` | **Nein.** Hier nennt sie zufällig die eigene Klasse, aber der Vertrag ist das JSON. Der Zieltyp kommt aus der Signatur der Listener-Methode, wie im `batch-writer`. |
| `delivery_mode` | `2` | nein |
| `priority` | `0` | nein |

Eine `message_id`-Property gibt es nicht. Die ID steht nur im Körper.

**Zur Kürzung von `sentAt`:** `Instant.now()` lieferte im Container neun Nachkommastellen
(gemessen: `…38.831525339Z`), die Datenbank speichert sechs (`…38.831525`). Dieselbe Nachricht
hätte live anders ausgesehen als beim Nachliefern. Behoben in Commit `52a2ea3`, gesichert durch
`MessageServiceTest.truncatesTimestampToMicroseconds`.

### 2.3 Ausgang: was am Stream hinausgeht

**Anfrage:** `GET /stream?roomId=<UUID>`, optional mit Kopfzeile `Last-Event-ID: <UUID>`.

**Kopfzeilen der Antwort:** `Content-Type: text/event-stream` (immer UTF-8),
`Cache-Control: no-cache`, keine `Content-Length`, `Transfer-Encoding: chunked`. Die
Verbindung bleibt offen.

**Ein Stream, Zeile für Zeile:**

```
retry:2000

id:3bf82e40-0d89-4e6a-a6ce-7cce4b0f1bb5
data:{"id":"3bf82e40-0d89-4e6a-a6ce-7cce4b0f1bb5","roomId":"39acf48b-5d49-40f0-a5d6-cd5bfdb59385","senderId":"s1","senderName":"Test","content":"Hallo","sentAt":"2026-11-13T09:09:09.251491Z"}

:ping

```

**Kein Leerzeichen nach dem Doppelpunkt.** Das sind die echten Bytes, die Spring schreibt:
In `SseEmitter$SseEventBuilderImpl` aus `spring-webmvc-6.2.19.jar` stehen die Literale `id:`,
`event:`, `retry:` und `data:` ohne Leerzeichen, ein Kommentar wird als `:` direkt vor dem Text
geschrieben (nachgeprüft am 13.11.2026). Nach dem SSE-Standard ist ein einzelnes Leerzeichen
nach dem Doppelpunkt optional und wird vom Client verworfen — beide Formen sind gültig, und
ich nehme die, die das Framework ohnehin liefert, statt den Writer von Hand nachzubauen.
Die Messmuster in Abschnitt 5 sind deshalb mit `: ?` gegen beide Formen tolerant.

| Element | Entscheidung |
|---|---|
| `id:` | Die Nachrichten-UUID. Der Browser schickt sie beim Neuverbinden als `Last-Event-ID` zurück. |
| `data:` | Das JSON aus 2.2, **mit demselben `ObjectMapper` serialisiert** — live wie beim Nachliefern. Deshalb entsteht auf beiden Wegen dieselbe Zeichenkette; die Kürzung auf Mikrosekunden ist nur die Voraussetzung dafür. Immer einzeilig, weil Jackson Zeilenumbrüche im Text als `\n` maskiert. |
| `event:` | Nein. Es gibt nur eine Ereignisart. Ohne `event:` landet alles in `onmessage` des Browsers. |
| `retry:` | Einmal am Anfang, `2000`. Damit ist die Wartezeit fürs Neuverbinden in S7 festgelegt und hängt nicht vom Browser ab. |
| `:ping` | Kommentar alle 15 Sekunden. Daran erkennt der Dienst tote Verbindungen (3.7.6). Ein Kommentar hat kein `id:` und ändert die `Last-Event-ID` nicht. |

| Anfrage | Antwort |
|---|---|
| ohne `roomId` oder keine UUID | `400 Bad Request` |
| unbekannte `roomId` | `200`, stiller Stream. Es gibt keine Raumliste, gegen die geprüft werden könnte. |
| `Last-Event-ID` ungültig oder aus einem anderen Raum | wie ohne Kopfzeile, mit WARN im Log |

---

## 3. Verhalten

### 3.1 Normalfall

```
POST /messages ─▶ MessageService.accept     id + sentAt vergeben
               ─▶ publish chat.persist       (Schreibweg, unverändert)
               ─▶ publish chat.delivery      (Fanout)
                     ├─▶ chat.stream.<A> ─▶ Instanz A: Listener
                     └─▶ chat.stream.<B> ─▶ Instanz B: Listener
                                              └─ Map[roomId] ─▶ emitter.send ─▶ data: beim Client
```

**Gefiltert wird im Dienst, nach dem Konsumieren.** Beim Binden geht es nicht: Der Fanout
ignoriert den Routing-Key, der Publisher setzt ohnehin `""`, und die `roomId` steht nur im
Körper. Filtern beim Binden ginge nur mit einem Topic-Exchange und der `roomId` als
Routing-Key — eine Änderung am vorgegebenen Vertrag. Der Preis meiner Lösung: Jede Instanz
parst jede Nachricht. Bei 1'667 Nachrichten pro Sekunde ist das billig.

**Im Speicher:** `Map<UUID roomId, Set<StreamConnection>>`. Eingetragen beim GET, ausgetragen
über `onCompletion`, `onTimeout` und `onError`.

**Reihenfolge:** ein Consumer pro Queue (`concurrency = 1`), und der Listener wirft nie — eine
Ausnahme würde ein Requeue auslösen und die Reihenfolge brechen. Was in einer Reihenfolge am
Exchange ankommt, geht in derselben Reihenfolge an den Client. Ein Absender, der sequenziell
sendet, behält seine Reihenfolge. **Nicht garantiert** ist eine Reihenfolge zwischen
verschiedenen Absendern (`PLANUNG.md`, offener Punkt 3).

### 3.2 S3 — zwei Instanzen, zwei Räume

**Aufbau laut Auftrag:** zwei Instanzen, an jeder ein Client auf Raum A, ein dritter Client auf
Raum B. Über jede Instanz gehen 200 Nachrichten für Raum A ein, zusammen 400. Beide Clients in
Raum A erhalten alle 400, jede genau einmal.

Jede Instanz hat eine eigene Queue am Fanout und sieht deshalb **jede** Nachricht — egal, über
welche Instanz sie eingeliefert wurde. Genau das zeigt dieser Aufbau: Der Client an Instanz 1
bekommt auch die 200 Nachrichten, die über Instanz 2 kamen, und umgekehrt. Bei einer
gemeinsamen Queue gälte Competing Consumers: Jede Nachricht ginge an nur eine Instanz, jeder der
beiden Raum-A-Clients sähe ungefähr die Hälfte. Der Client in Raum B bekommt nichts, weil der
Filter aus 3.1 die Nachrichten aus Raum A verwirft.

**Reihenfolge:** Es gibt zwei Absender (einer pro Instanz). Garantiert ist die Reihenfolge
*je Absender*, nicht die Verzahnung der beiden (3.1).

### 3.3 S4 — von zwei Instanzen zurück auf eine

Bei `--scale chat-service=1` bekommt der überzählige Container ein SIGTERM:

1. Bei `ContextClosedEvent` schliesst der Dienst alle Emitter mit `complete()`.
2. Die Browser verbinden sich mit `Last-Event-ID` neu und landen bei der verbleibenden Instanz
   (3.7).
3. Spring schliesst die AMQP-Verbindung, die Queue hat keinen Consumer mehr.
4. Nach `x-expires = 10000` löscht RabbitMQ die Queue.

**Warum Schritt 1 Pflicht ist:** `server.shutdown` steht in Spring Boot 3.5.16 standardmässig
auf `graceful`, mit `spring.lifecycle.timeout-per-shutdown-phase = 30s` (nachgesehen in den
Konfigurations-Metadaten von `spring-boot-autoconfigure-3.5.16`). Offene SSE-Streams sind
laufende Requests. Ohne Schritt 1 würde Tomcat auf sie warten, bis Docker nach 10 Sekunden
mit SIGKILL beendet.

**Verzögerung:** Herunterfahren ohne offene Streams dauert heute **0,4–0,5 s** (dreimal
gemessen mit `docker compose stop chat-service`). Dazu kommen 10 s `x-expires` und eine
Unschärfe des Brokers, für die RabbitMQ keine Zusage macht. Erwartet: deutlich unter 15 s.
Limit: 30 s.

**Nachgemessen in L7 (13.11.2026):** fünf Läufe mit je einem offenen Client an der
verschwindenden Instanz — **13, 13, 11, 13, 13 Sekunden** vom `--scale chat-service=1` bis zum
Verschwinden der Queue. Das bestätigt die Rechnung: rund 0,5 s Herunterfahren, 10 s
`x-expires`, der Rest Unschärfe des Brokers. Die Erwartung „deutlich unter 15 s" trifft zu, und
der Abstand zum Limit beträgt mehr als das Doppelte.

Dass die offenen Streams dabei nicht bremsen, zeigt die Rechnung selbst: Ohne das Schliessen
beim `ContextClosedEvent` würde Tomcat auf sie warten, bis Docker nach zehn Sekunden mit
SIGKILL beendet — die Summe läge dann bei gut 20 s statt bei 13.

**Warum `chat.persist` und `chat.dlq` bestehen bleiben, die Stream-Queues aber nicht:**
`chat.persist` hält die einzige Kopie auf dem Weg in die Datenbank — Arbeit, die erledigt
werden muss, egal wer gerade konsumiert. `chat.dlq` hält Beweismaterial. Eine Stream-Queue ist
ein Puffer für **genau einen** Verbraucher; ohne ihn sind ihre Kopien wertlos.

### 3.4 S5 — Datenbank und `batch-writer` gestoppt

Der Anzeigeweg aus 3.1 berührt die Datenbank nicht. Alle 100 Nachrichten kommen beim Client
an. Die Datenbank braucht der Dienst **nur** fürs Nachliefern (3.7).

Damit das auch gilt, wenn der `chat-service` **neu startet, während Postgres unten ist**:

- **kein** `depends_on: postgres` mit `service_healthy`,
- kein Flyway, keine SQL-Skripte beim Start,
- **`spring.datasource.hikari.initialization-fail-timeout: -1`** (4.3). HikariCP steht
  standardmässig auf `1`: Gelingt beim Aufbau des Pools die erste Verbindung nicht, wirft er.
  Nach meinem Stand baut Spring Boot den Pool zwar erst beim ersten Zugriff auf, aber darauf
  will ich mich nicht verlassen — ein einziger Bean, der beim Start die Datenbank berührt,
  würde das ändern. Mit `-1` versucht Hikari beim Start gar nicht erst zu verbinden.
  Der erste Zugriff schlägt dann zur Laufzeit fehl, und genau diesen Fall fängt Schritt 8 in
  3.7.2 ab. Nachgewiesen wird es in L8.

**Dass danach alle 100 in der Tabelle stehen**, ist die Arbeit des `batch-writer` aus
Bewertung 1: `chat.persist` ist durable, die Nachrichten sind persistent und warten in der
Queue. Der `batch-writer` bestätigt einen Stapel erst nach dem Commit
(`MessageBatchListener.java:75`); Duplikate fängt `ON CONFLICT DO NOTHING` ab
(`MessageRepository.java:40-44`).

### 3.5 S6 — eine Instanz angehalten

Der Prozess ist eingefroren, die TCP-Verbindung zum Broker steht weiter. Für den Broker hat die
Queue weiterhin einen Consumer. Der Fanout legt weiter Kopien hinein: bis zu 50 unbestätigt im
Socket der Instanz (Prefetch), alles Weitere als „ready“ in der Queue.

**Grenze:** `x-max-length = 950` mit `x-overflow = drop-head`. `x-max-length` zählt **nur
„ready“**, nicht die unbestätigten Nachrichten. Mit Prefetch 50 sind es zusammen höchstens
**1000**.

**Am Limit** fliegt die **älteste** Nachricht, still:

- In einem Live-Chat ist die neueste Nachricht die wertvollste.
- Die verworfene liegt in `chat.persist` und landet in der Tabelle.
- `reject-publish` würde die neue abweisen. Nach dem Fortsetzen kämen dann zuerst die
  1000 ältesten.
- Ein Abstellgleis würde selbst unbegrenzt wachsen.

Der Absender merkt nichts: `drop-head` sendet kein nack, und es gibt keine Publisher Confirms.

**Nach dem Fortsetzen** sendet die Instanz die bis zu 1000 gepufferten Nachrichten und läuft
dann live weiter — neue Nachrichten kommen wieder an, nicht alle 5000.

**Warum die Grenze nötig ist:** Ohne sie wächst die Queue im RAM des Brokers, bis der
Memory-Alarm auslöst. Dann **blockiert RabbitMQ alle Publisher**, auch den Schreibweg. Eine
einzige hängende Instanz würde das ganze System anhalten.

### 3.6 Der Widerspruch zwischen S4 und S6

Eine pausierte Instanz antwortet nicht mehr auf Heartbeats. Der Broker schliesst ihre
Verbindung nach ungefähr zwei ausgebliebenen Heartbeats. Ausgehandelt wird das Minimum aus
Client- und Broker-Wert; gemessen am 13.11.2026 mit `rabbitmqctl list_connections peer_host
timeout`: **60 s** für `batch-writer` und `chat-service`. Heartbeats gehen alle 30 s, die
Verbindung schliesst also ungefähr **60–120 s** nach `pause`.

**Verworfen: `exclusive` (Spring `AnonymousQueue`) mit langem Heartbeat für alle.** Eine
exclusive Queue verschwindet mit ihrer Verbindung, in S6 also nach Ablauf der Heartbeat-Frist.
Retten liesse sich das nur mit einem langen Heartbeat. Wäre der global, hinge ein stumm
gestorbener `batch-writer` bis zu 10 Minuten mit 500 unbestätigten Nachrichten fest statt
einer Minute — eine Verschlechterung am Schreibweg aus Bewertung 1.

**Gewählt: `x-expires`, nicht `exclusive`, nicht `auto-delete`.**

- **S4:** Beim sauberen Ende verschwindet der Consumer, die Queue ist nach 10 s weg.
- **S6:** Solange die Verbindung steht, ist ein Consumer registriert, die Queue bleibt. Erst
  ab dem Schliessen durch den Heartbeat läuft die Uhr.

**Gemessen:** 5000 Nachrichten sequenziell über `wget` an eine Instanz brauchen **14,9 s**
(13.11.2026). Mit dem Standard-Heartbeat (Fenster 60–120 s + 10 s) reicht das — aber nur, wenn
das Prüfskript ähnlich schnell sendet. Darauf will ich S6 nicht bauen.

**Entscheidung: Heartbeat für den `chat-service` auf 300 s, nur für ihn.** Ausgehandelt wird
pro Verbindung das Minimum aus Client- und Broker-Wert. Eine Zeile im `chat-service` allein
bewirkt deshalb nichts — der Broker bliebe bei 60 und gewänne. Es braucht drei Stellen:

| Stelle | Einstellung | Ergebnis |
|---|---|---|
| Broker (`docker-compose.yml`, Dienst `rabbitmq`) | `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: "-rabbit heartbeat 300"` | Obergrenze 300 |
| `chat-service/application.yml` | `spring.rabbitmq.requested-heartbeat: 300s` | min(300, 300) = **300** |
| `batch-writer/application.yml` | `spring.rabbitmq.requested-heartbeat: 60s` | min(60, 300) = **60** — der Schreibweg bleibt wie in Bewertung 1 |

Die Zeile im `batch-writer` ist die einzige Änderung an diesem Dienst. Ohne sie hinge er an
einem Standardwert der Bibliothek, den niemand im Code sieht.

**Fenster für S6 damit:** 300–600 s + 10 s. Das reicht für jede realistische Pause.

**Preis:** Eine `chat-service`-Instanz, die **stumm** stirbt (Netz weg, Container eingefroren
und nie fortgesetzt), behält ihre Queue bis zu ungefähr 10 Minuten. Harmlos, weil die Queue auf
1000 Nachrichten gedeckelt ist (3.5) — die Grenze macht den langen Heartbeat erst vertretbar.
S4 ist nicht betroffen: Dort wird sauber heruntergefahren, die Verbindung schliesst sofort.

Nachgewiesen wird die Aushandlung in L14.

**Warum genau 10 Sekunden:** Nach oben begrenzt S4 — die Unschärfe des Brokers braucht Luft bis
30 s. Nach unten muss der Wert über dem `recoveryInterval` von Spring (5 s) liegen, damit nach
einem kurzen Netzaussetzer mindestens ein Neuverbindungsversuch durchkommt. Für S6 kostet ein
kleinerer Wert fast nichts, weil die Heartbeat-Frist dort der grössere Teil ist.


### 3.7 Verhalten bei Neuverbindung (S7)

#### 3.7.1 Zustand pro Verbindung

`StreamConnection` hält:

- `SseEmitter emitter`
- `UUID roomId`
- `Instant registeredAt`
- Zustand `CATCHING_UP` oder `LIVE`
- `List pending` — live eingetroffen, noch nicht gesendet, höchstens 1000
- `Set sentIds` — beim Nachliefern gesendete IDs, zeitlich begrenzt (3.7.5)
- eine eigene Sperre

#### 3.7.2 Abfolge von `GET /stream` bis `LIVE`

1. **Anfrage prüfen** (Request-Thread). `roomId` fehlt oder ist keine UUID → `400`, Ende.
   `Last-Event-ID` lesen. Fehlt der Header oder ist er keine UUID, gilt der Anker als „keiner“.
2. **Emitter erzeugen:** `new SseEmitter(0L)`. Nach der Servlet-Spezifikation bedeutet `0`
   kein Timeout. `onCompletion`, `onTimeout` und `onError` tragen die Verbindung in jedem Fall
   aus der Map aus.
3. **Verbindung eintragen — vor jeder Datenbankabfrage** — und `registeredAt` merken. Mit Anker
   ist der Zustand `CATCHING_UP`, ohne Anker `LIVE`. Ab jetzt landet jede live eintreffende
   Nachricht des Raums in `pending` oder direkt im Stream.
4. **`retry:2000` senden.**
5. **Ohne Anker: fertig.** Der Controller gibt den Emitter zurück.
6. **Mit Anker: Nachlieferung einplanen.**
   - Laufen schon 200 Nachlieferungen (wartend oder aktiv): Emitter mit `complete()`
     schliessen, WARN, Ende. Der Browser versucht es nach 2 s erneut.
   - Sonst die Nachlieferung mit 1 s Verzögerung im `ThreadPoolTaskScheduler` (4 Threads)
     einplanen und den Emitter sofort zurückgeben.
7. **Verzögerung, 1 Sekunde.** In dieser Zeit schreibt der `batch-writer` mit seinem
   Sammelfenster von 200 ms (`batch-writer/.../config/RabbitConfig.java:34`) weg, was vor
   Schritt 3 veröffentlicht wurde. Die Verzögerung ist eingeplant; kein Thread wartet dafür.
8. **Anker suchen:** `SELECT sent_at FROM message WHERE id = ? AND room_id = ?`.
   - Nicht gefunden: bis zu dreimal im Abstand von 500 ms wiederholen.
   - Immer noch nicht gefunden oder Datenbank nicht erreichbar: WARN und weiter mit Schritt 11.
9. **Eine Seite nachliefern:** Abfrage aus 3.7.3 mit `LIMIT 500`. Jede Zeile senden. Ist ihr
   `sent_at` nicht älter als `registeredAt − 5 s`, ihre ID in `sentIds` eintragen.
10. **Weiterblättern:** Hatte die Seite 500 Zeilen, wird die letzte Zeile zum neuen Anker,
    zurück zu Schritt 9.
11. **Umschalten** (unter der Sperre). Jede Nachricht aus `pending` in der Reihenfolge ihres
    Eintreffens senden, ausser ihre ID steht in `sentIds`. `pending` leeren, Zustand `LIVE`,
    `sentIds` verwerfen.
12. **Live-Betrieb** (Listener-Thread, unter der Sperre). Pro eintreffender Nachricht:
    - `LIVE`: sofort senden.
    - `CATCHING_UP`: an `pending` anhängen.
    - `CATCHING_UP` und `pending` hat schon 1000 Einträge: **Emitter mit `complete()`
      schliessen**, WARN. Der Browser verbindet sich nach 2 s mit der letzten ID, die er
      *wirklich* bekommen hat, und die Nachlieferung beginnt dort neu.

Daneben laufen:

- **Ping:** Ein `@Scheduled`-Job schickt alle 15 s `:ping` an jede Verbindung.
- **Herunterfahren:** Bei `ContextClosedEvent` alle Emitter mit `complete()` schliessen (3.3).

#### 3.7.3 Die Abfrage

```sql
SELECT id, room_id, sender_id, sender_name, content, sent_at
FROM message
WHERE room_id = ?
  AND sent_at >= ?                -- für den Index
  AND (sent_at, id) > (?, ?)      -- für die Korrektheit
ORDER BY sent_at, id
LIMIT 500
```

- Die Tabelle hat keine Sequenznummer. „Nach dieser ID“ heisst: nach dem Tupel `(sent_at, id)`
  des Ankers.
- Bei gleicher Zeitangabe bricht `id` den Gleichstand. Die Ordnung der UUIDs ist willkürlich,
  aber stabil — nie doppelt, nie übersprungen. Gemessen: 5000 Nachrichten in 15 s hatten keine
  einzige gleiche Zeitangabe.
- Weitergeblättert wird per Keyset: Die letzte Zeile ist der neue Anker. Auch nach langer
  Abwesenheit liegen höchstens 500 Zeilen im Speicher.
- Die Zeilen werden in denselben `ChatMessage`-Record umgewandelt und mit demselben
  `ObjectMapper` serialisiert wie live.

#### 3.7.4 Index

Der vorhandene Index `(room_id, sent_at DESC)` enthält `id` nicht. **Entscheidung: so lassen,
keine V2-Migration.** Das Schema gehört dem `batch-writer`; eine Migration dort liegt ausserhalb
dieses Auftrags.

Die Bedingung `sent_at >= ?` macht den Index benutzbar. **Gemessen am 13.11.2026** mit 5053
Zeilen, alle in einem Raum:

| Planer | Plan | Zeit |
|---|---|---|
| frei | `Seq Scan` + `Sort` (top-N heapsort) | 0,8 ms |
| `enable_seqscan = off` | `Bitmap Index Scan on idx_message_room_sent_at` + `Sort` | 0,9 ms |
| zusätzlich `enable_bitmapscan = off` | `Index Scan Backward using idx_message_room_sent_at` + `Incremental Sort`, stoppt nach 501 Zeilen | 0,2 ms |

Der Index ist also benutzbar, und der Wunschplan ist möglich. Welchen Plan Postgres wählt,
hängt an der Datenmenge: Liegen alle Zeilen in einem Raum, ist der Seq Scan die richtige Wahl.
Ein eindeutiger Plan entstünde erst mit einem Index `(room_id, sent_at, id)` (Abschnitt 6).

#### 3.7.5 Warum nichts verloren geht und nichts doppelt kommt

| Wann veröffentlicht | Woher der Client sie bekommt |
|---|---|
| vor Schritt 3, schon geschrieben | Datenbank |
| vor Schritt 3, noch nicht geschrieben | Datenbank, dank der 1 s aus Schritt 7. Staut der `batch-writer`, fehlt sie (Abschnitt 6). |
| nach Schritt 3 | `pending`. Steht sie zugleich in der Datenbank, verhindert `sentIds` das Duplikat. |
| nach Schritt 11 | live |

**Warum `sentIds` nur die letzten 5 s hält:** Alles in `pending` wurde nach Schritt 3
veröffentlicht, und `sentAt` wird höchstens Millisekunden vor dem Veröffentlichen gesetzt. Nur
Zeilen mit `sent_at ≥ registeredAt − 5 s` können überhaupt doppelt sein. So bleibt das Set
auch nach einer Stunde Abwesenheit klein.

Die Sperre pro Verbindung sorgt dafür, dass eine Nachricht, die genau während Schritt 11
eintrifft, weder vorgezogen wird noch verloren geht.

**Andere Instanz beim Neuverbinden:** spielt keine Rolle. Jede Instanz sieht alle Nachrichten,
die Datenbank teilen alle.

#### 3.7.6 Wie der Dienst merkt, dass ein Client weg ist

Erst beim Schreiben: Tomcat wirft `ClientAbortException`, `onError` trägt die Verbindung aus.
Damit das auch in stillen Räumen passiert, gibt es den Ping alle 15 s.

#### 3.7.7 Grenzen auf einen Blick

| Was | Grenze | Beim Erreichen |
|---|---|---|
| Stream-Queue | 950 „ready“ + 50 Prefetch | älteste verwerfen (3.5) |
| `pending` pro Verbindung | 1000 | Emitter schliessen, Browser verbindet neu ab letzter erhaltener ID |
| `sentIds` pro Verbindung | nur Zeilen der letzten 5 s vor der Registrierung | — |
| Nachlieferungen gleichzeitig | 200, auf 4 Threads | Emitter schliessen, Browser versucht es nach 2 s erneut |
| Datenbankverbindungen | Hikari-Standard 10, mehr als die 4 Threads | — |

---

## 4. Queues und Konfiguration

### 4.1 Die Stream-Queue

| Eigenschaft | Wert | Begründung |
|---|---|---|
| Name | `chat.stream.<UUID>` | Präfix macht die Queues zählbar, UUID macht jede pro Instanz eindeutig. **Kein Hostname:** Ein neu gestarteter Container würde sonst eine alte Queue mit veralteten Nachrichten übernehmen. Sie kämen nach dem Ende der Nachlieferung an — als Duplikate. |
| Wer den Namen vergibt | die `@Bean`-Methode, **einmal pro Prozess** | `RabbitAdmin` deklariert bei jeder Neuverbindung **dasselbe** Objekt. Nach einem Verbindungsabbruch (S6) findet die Instanz ihre alte Queue mit dem gepufferten Inhalt wieder. Der Listener liest den Namen mit `@RabbitListener(queues = "#{liveQueue.name}")`. |
| Log beim Start | `Live queue chat.stream.<UUID> declared` | Ordnet in der Abnahme jeder Instanz ihre Queue zu. |
| Entstehung | beim Start, bei jeder Neuverbindung erneut | Nicht erst beim ersten Client: einfacher, kein Zustand „Queue fehlt noch“. |
| `exclusive` / `auto-delete` | nein / nein | Beide würden die Queue löschen, sobald der Heartbeat die Verbindung schliesst (3.6). Deshalb auch keine `AnonymousQueue` — die ist immer beides. |
| `durable` | **ja** | Nicht weil der Inhalt wertvoll wäre, sondern aus zwei harten Gründen: RabbitMQ 3.13 führt nicht-exklusive, nicht-durable Classic Queues als veraltete Funktion, in 4.0 fallen sie weg. Und `durable` lässt sich nachträglich nicht ändern — eine abweichende Deklaration scheitert mit `PRECONDITION_FAILED`. Der Preis ist praktisch null: höchstens 1000 kurzlebige Nachrichten erzeugen kaum Plattenzugriffe, und `x-expires` räumt trotzdem auf. **Anders als bei `chat.persist`:** Dort ist `durable` nötig, weil die Queue die einzige Kopie hält. Hier ist es die zukunftssichere Form. |
| `x-expires` | `10000` | Aufräumen nach 10 s ohne Consumer (3.6). |
| `x-max-length` | `950` | zusammen mit Prefetch höchstens 1000 (3.5) |
| `x-overflow` | `drop-head` | älteste verwerfen (3.5) |
| Prefetch | `50` | begrenzt, was unbestätigt beim Consumer liegt |
| Dead-Letter-Queue | **keine** | Überlauf ist kein Defekt. Eine DLQ würde unbegrenzt wachsen, und das Original läuft über `chat.persist`. |

Die Argumente müssen bei jeder Deklaration exakt gleich sein. Werte aus `.env` ändern sich
deshalb nur mit einem Neustart — und ein Neustart bringt einen neuen Namen.

### 4.2 Umgebungsvariablen

Neu in `.env.example`:

```
# Live-Zustellung im chat-service. Die ersten beiden zusammen duerfen
# 1000 nicht ueberschreiten (Auftrag S6).
STREAM_QUEUE_MAX_LENGTH=950
STREAM_PREFETCH=50
STREAM_QUEUE_EXPIRES_MS=10000
```

`POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD` und `POSTGRES_DB` gibt es schon; sie
gehen jetzt auch an den `chat-service`. In der `application.yml` stehen für alle Werte
Vorgaben, damit der Dienst auch ohne `.env` startet.

### 4.3 Feste Einstellungen im Dienst

| Einstellung | Wert |
|---|---|
| `retry:` | 2000 ms |
| Ping-Intervall | 15 s |
| Verzögerung vor der Nachlieferung | 1 s |
| Anker-Wiederholungen | 3 × 500 ms |
| Seitengrösse beim Nachliefern | 500 |
| `pending` höchstens | 1000 |
| Nachlieferungen höchstens / Threads | 200 / 4 |
| Listener-`concurrency` | 1 |
| `spring.datasource.hikari.initialization-fail-timeout` | `-1` — Start ohne Datenbank (3.4) |
| `spring.rabbitmq.requested-heartbeat` | `300s` — S6 (3.6) |

### 4.4 Änderungen an bestehenden Dateien

- `docker-compose.yml`, Block `chat-service`: `POSTGRES_*` und `STREAM_*` ergänzen,
  `depends_on` nur auf `rabbitmq`, **kein** `ports:`.
- `docker-compose.yml`, Block `rabbitmq`: `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: "-rabbit heartbeat 300"` (3.6).
- `chat-service/src/main/resources/application.yml`: `requested-heartbeat: 300s`,
  Datenquelle mit `initialization-fail-timeout: -1`.
- `batch-writer/src/main/resources/application.yml`: `requested-heartbeat: 60s` (3.6).
- `RabbitConfig.java:41-44` und `QueueNames.java:15`: Die Kommentare nennen noch
  „web-gateway“ und „WebSocket“ — nachziehen.
- `PLANUNG.md` 3.4: Hinweis auf die Abweichung aus Abschnitt 1.

---

## 5. Abnahmekriterien

Kein Container veröffentlicht einen Port. Gemessen wird **von innen**.

### Genau eine Instanz ansprechen

Der Dienstname `chat-service` wird von Docker reihum verteilt. Der **Containername** ist im
Netz `chat-net` ein eigener DNS-Name und zeigt immer auf genau eine Instanz:

```bash
docker compose ps --format '{{.Name}}'
```

erwartet: `it3c-m321-chat-service-1` und `it3c-m321-chat-service-2`.

### SSE-Client

`-N` schaltet die Pufferung ab.

```bash
docker run --rm --network chat-net curlimages/curl:8.10.1 -sN --max-time 60 "http://it3c-m321-chat-service-1:8080/stream?roomId=$ROOM_A" > a1.txt
```

Mit `Last-Event-ID`, hier an der anderen Instanz:

```bash
LAST=$(grep '^id:' a1.txt | tail -1 | cut -d: -f2- | tr -d ' ')
```

Getrennt wird am **Doppelpunkt**, nicht am Leerzeichen: Spring schreibt `id:<UUID>` ohne
Leerzeichen (2.3), `cut -d' ' -f2` gäbe eine leere ID, und der Header ginge leer hinaus —
still, ohne Fehlermeldung. `tr -d ' '` fängt zusätzlich die Form mit Leerzeichen ab.

```bash
docker run --rm --network chat-net curlimages/curl:8.10.1 -sN --max-time 60 -H "Last-Event-ID: $LAST" "http://it3c-m321-chat-service-2:8080/stream?roomId=$ROOM_A" > a2.txt
```

### Nachrichten erzeugen

An eine bestimmte Instanz (wichtig für S6, weil eine pausierte Instanz keine POSTs annimmt und
`docker compose exec` ohne `--index` die Instanz 1 nimmt):

```bash
docker compose exec -T --index 2 -e N=200 -e ROOM="$ROOM_A" -e SENDER=s2 -e TAG=zwei chat-service sh -c '
  i=1
  while [ $i -le $N ]; do
    wget -qO- --header="Content-Type: application/json" \
      --post-data="{\"roomId\":\"$ROOM\",\"senderId\":\"$SENDER\",\"senderName\":\"Test\",\"content\":\"$TAG $i\"}" \
      http://localhost:8080/messages > /dev/null
    i=$((i+1))
  done'
```

`--index` wählt die Instanz, `SENDER` und `TAG` unterscheiden die Absender. Für S3 läuft
derselbe Befehl ein zweites Mal mit `--index 1 -e SENDER=s1 -e TAG=eins`.

### Auswertung

Für eine Ausgabedatei `f` eines SSE-Clients:

```bash
grep -c '^id:' f
```

```bash
grep '^id:' f | sort | uniq -d | wc -l
```

```bash
grep '^data:' f | grep "\"content\":\"$TAG " | sed 's/.*"content":"[^ ]* \([0-9]*\)".*/\1/' | awk '$1<=p{b++}{p=$1}END{print b+0}'
```

Anzahl, Duplikate (erwartet `0`), Reihenfolgefehler **je Absender** (`TAG` setzen, erwartet `0`).

Ob zwei Clients genau dieselben Nachrichten bekommen haben:

```bash
diff <(grep '^id:' a1.txt | sort) <(grep '^id:' a2.txt | sort) | wc -l
```

erwartet: `0`.

Queues und Bindungen am Fanout:

```bash
docker compose exec -T rabbitmq rabbitmqctl -q list_queues name durable consumers messages_ready messages_unacknowledged arguments | grep chat.stream
```

```bash
docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name | grep '^chat.delivery'
```

### Kriterien

| Nr | Kriterium | Messung | Erwartet |
|---|---|---|---|
| **L1** | Alle Tests grün | `mvn clean test` im Wurzelverzeichnis | `BUILD SUCCESS` |
| **L2** | Kein Port veröffentlicht | `docker compose ps --format "{{.Service}}: {{.Publishers}}"` | bei keinem Dienst eine Host-Portnummer |
| **L3** | Eine Queue pro Instanz, richtig konfiguriert | `--scale chat-service=2`, dann Queues und Bindungen | 2 × `chat.stream.*`, je `consumers = 1`, `durable = true`, Argumente `x-expires 10000`, `x-max-length 950`, `x-overflow drop-head`; 2 Bindungen an `chat.delivery` |
| **L4** | SSE-Antwort korrekt | `curl -sN -D - --max-time 3 …/stream?roomId=$ROOM_A` | `Content-Type: text/event-stream`, erste Körperzeile trifft `grep -E '^retry: ?2000'` |
| **L5** | Fehlende oder ungültige `roomId` | `curl -s -o /dev/null -w '%{http_code}'` auf `…/stream` und `…?roomId=abc` | je `400` |
| **L6** | Normalfall und S3 | `--scale chat-service=2`. Client A1 an Instanz 1 und Client A2 an Instanz 2, beide Raum A; Client B an Instanz 1, Raum B. 200 an Raum A über Instanz 1 (`TAG=eins`) **und** 200 über Instanz 2 (`TAG=zwei`) | `a1.txt` und `a2.txt`: je **400** IDs, 0 Duplikate, je Absender 0 Reihenfolgefehler, `diff` der ID-Listen = `0`. `b.txt`: `grep -c '^data:'` = `0` **und** `grep -cE '^: ?ping'` > 0 — der Ping beweist, dass die Verbindung offen war |
| **L7** | S4: Queue weg in ≤ 30 s | 2 Instanzen mit je einem Client, `docker compose up -d --scale chat-service=1`, danach jede Sekunde die Queues zählen, mit Zeitstempel. **Fünfmal.** | grösster Wert ≤ 30 s; der Client hat sich neu verbunden |
| **L8** | S5: Anzeige ohne Datenbank | `docker compose stop postgres batch-writer`, **dann `docker compose restart chat-service`**, Client an, 100 senden, `docker compose start postgres batch-writer` | `chat-service` kommt hoch (`docker compose logs chat-service \| grep "Started ChatServiceApplication"`). Client: 100 IDs. Nach ≤ 60 s zeigt `SELECT count(*) FROM message` 100 mehr als vorher |
| **L9** | S6: Grenze und Wiederanlauf | Client an Instanz 1; Queue von Instanz 1 über die Log-Meldung zuordnen; `docker compose pause` für Instanz 1; **mitlaufend** jede Sekunde `messages_ready + messages_unacknowledged` aller `chat.stream.*` in `s6.log`; 5000 an Instanz 2; `unpause`; 10 mit `TAG=nachher` | Höchstwert in `s6.log` ≤ 1000; die Queue von Instanz 1 existiert nach dem Fortsetzen noch; der Client hat innerhalb von 30 s alle 10 `nachher` |
| **L10** | S7: Neuverbinden, keine verloren, keine doppelt | 300 an Raum A senden; parallel Client 1 mit `--max-time 3`, danach sofort Client 2 mit `Last-Event-ID` **an der anderen Instanz** | `a1.txt` + `a2.txt`: 300 IDs, 0 Duplikate, 0 Reihenfolgefehler; `data:` derselben ID ist in beiden Wegen gleich |
| **L11** | Keine veraltete Queue-Art | `docker compose logs rabbitmq \| grep -i deprecat` | kein Treffer zu transienten Queues |
| **L12** | Nachliefer-Abfrage nutzt den Index | `EXPLAIN ANALYZE` der Abfrage aus 3.7.3 über `psql`, mit Daten in mehreren Räumen | `idx_message_room_sent_at` erscheint im Plan, sobald der Raum nur ein kleiner Teil der Tabelle ist; Laufzeit pro Seite < 10 ms |
| **L13** | Regeln aus `CLAUDE.md` | `grep -rn "\.stream()" chat-service/src` und Sichtprüfung | keine Treffer; Kommentar über jeder Klasse und Methode |
| **L14** | Heartbeat nur für den `chat-service` verlängert | `docker compose exec -T rabbitmq rabbitmqctl -q list_connections peer_host timeout`, Adressen mit `docker network inspect chat-net` zuordnen | `chat-service`: `300`, `batch-writer`: `60` |

**Zu L9 — „nie über 1000“:** Eine Abtastung jede Sekunde kann eine Spitze zwischen zwei
Messpunkten verpassen. Den Beweis führen deshalb der Mechanismus (Argumente in L3) und die
Messwerte zusammen, nicht die Messung allein.

**Zuordnung:** L6 = Normalfall und S3, L7 = S4, L8 = S5, L9 und L14 = S6, L10 = S7.

### Bereits gemessen (13.11.2026)

Vor dem Bauen, aus der Spezifikationsarbeit:

| Messung | Ergebnis | Wo verwendet |
|---|---|---|
| Kopfzeilen am Fanout | `delivery_mode 2`, `content_type application/json`, `content_encoding UTF-8`, `__TypeId__`, `priority 0` | 2.2 |
| Genauigkeit `sentAt` vorher / nachher | 9 / 6 Nachkommastellen | 2.2, Commit `52a2ea3` |
| Heartbeat (vor der Änderung aus 3.6) | 60 s für `batch-writer` und `chat-service` | 3.6 |
| 5000 Nachrichten senden | 14,9 s | 3.6 |
| Herunterfahren `chat-service` ohne Streams | 0,4–0,5 s (3 Läufe) | 3.3 |
| `server.shutdown` in Spring Boot 3.5.16 | `graceful`, 30 s pro Phase | 3.3 |
| Abfrageplan, 5053 Zeilen in **einem** Raum | siehe 3.7.4 | 3.7.4, L12 |

### Abnahme (13.11.2026, 19:00–21:00)

| Nr | Erwartet | Gemessen | |
|---|---|---|---|
| **L1** | `BUILD SUCCESS` | `BUILD SUCCESS`, **64 Tests**, 0 Fehler, 0 übersprungen (53 `chat-service`, 11 `batch-writer`) | ✓ |
| **L2** | keine Host-Portnummer | alle **11** Port-Einträge des Projekts mit `PublishedPort: 0` | ✓ |
| **L3** | 2 × `chat.stream.*`, je `consumers = 1`, `durable`, drei Argumente, 2 Bindungen | 2 Queues, je `true` / `1` / `[{"x-expires",10000},{"x-overflow","drop-head"},{"x-max-length",950}]`, 2 Bindungen an `chat.delivery` | ✓ |
| **L4** | `text/event-stream`, erste Körperzeile `retry: ?2000` | `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Transfer-Encoding: chunked`, **keine** `Content-Length`; erste Körperzeile `retry:2000` | ✓ |
| **L5** | je `400` | ohne `roomId`: `400`; `roomId=abc`: `400` | ✓ |
| **L6** | je 400 IDs, 0 Duplikate, 0 Reihenfolgefehler je Absender, `diff` = 0; `b.txt` 0 × `data:` und > 0 × `:ping` | `a1.txt` **400**, `a2.txt` **400**, je 0 Duplikate, je Absender 200 Nachrichten und **0** Reihenfolgefehler, `diff` = **0**; `b.txt` **0** × `data:`, **4** × `:ping` | ✓ |
| **L7** | grösster Wert ≤ 30 s | fünf Läufe mit je einem Client an der verschwindenden Instanz: **13, 13, 11, 13, 13 s** | ✓ |
| **L8** | Start ohne DB, 100 IDs, nach ≤ 60 s 100 Zeilen mehr | `Started ChatServiceApplication in 3.796 seconds` bei gestoppter DB und **keiner** Postgres-Meldung im Log; Client **100** IDs, 0 Duplikate, 0 Reihenfolgefehler; `chat.persist` hielt zeitgleich **100** `messages_ready`; nach dem Start +100 Zeilen in **12 s** | ✓ |
| **L9** | Höchstwert ≤ 1000; Queue überlebt; 10 × `nachher` in ≤ 30 s | Höchstwert einer einzelnen Queue in `s6.log`: **1000**, exakt die Grenze; Client der pausierten Instanz **1010** IDs (1000 gepuffert + 10 `nachher`), 0 Duplikate | ✓ |
| **L10** | 300 IDs, 0 Duplikate, 0 Reihenfolgefehler | `a1` + `a2` zusammen **300** IDs, **300 verschiedene**, 0 Duplikate | ✓ |
| **L11** | kein Treffer zu transienten Queues | 0 Treffer auf `transient` oder `classic_queue`; die drei Treffer auf „deprecat" betreffen `management_metrics_collection` | ✓ |
| **L12** | Index im Plan, < 10 ms pro Seite | 17 182 Zeilen in 3 Räumen. Kleiner Raum (300 Zeilen, 1,7 %): `Bitmap Index Scan on idx_message_room_sent_at`, **0,314 ms**. Grosser Raum (10 020 Zeilen, 58 %): `Index Scan Backward using idx_message_room_sent_at` + `Incremental Sort`, stoppt nach 501 Zeilen, **0,583 ms** | ✓ |
| **L13** | keine Treffer | `grep -rn "\.stream()" chat-service/src`: **0** Treffer bei 21 Java-Dateien | ✓ |
| **L14** | `chat-service` 300, `batch-writer` 60 | `172.18.0.2` (`chat-service`) → **300**, `172.18.0.5` (`batch-writer`) → **60** | ✓ |

**Zu L12:** Der Wunschplan aus 3.7.4 — `Index Scan Backward` mit `Incremental Sort` — stellte
sich bei echten Daten in mehreren Räumen **von selbst** ein. In 3.7.4 war er nur mit
`enable_seqscan = off` zu sehen, weil dort alle 5053 Zeilen in einem einzigen Raum lagen. Die
Bedingung `sent_at >= ?` tut also genau, was sie soll: Sie erscheint in beiden Plänen als
`Index Cond`. Offener Punkt 9 (Index ohne `id`) bleibt damit bestehen, verliert aber an
Gewicht.

**Zu L2:** Ein erster Versuch mit `docker ps` statt `docker compose ps` zählte zwei
Veröffentlichungen — beide gehörten zu Testcontainers aus dem gleichzeitig laufenden L1-Build,
nicht zum Stack. Gemessen wird deshalb nur im Compose-Projekt.

### Tests, die diese Kriterien absichern

| Test | Sichert ab | Art |
|---|---|---|
| `MessageServiceTest.truncatesTimestampToMicroseconds` | 2.2 — `sentAt` höchstens Mikrosekunden | Unit, **vorhanden** |
| Queue-Deklaration | L3 — Name, `durable`, Argumente | echter Broker (Testcontainers) |
| Filter nach Raum | L6 — Raum B bekommt nichts | Unit |
| SSE-Format | 2.3 — `retry:`, `id:`, `data:` einzeilig | Integration |
| Nachliefern ohne Duplikat | 3.7.5 — Überschneidung von `pending` und Datenbank | Integration mit echter DB |
| `pending` am Limit | 3.7.2 Schritt 12 — Emitter wird geschlossen | Unit |

---

## 6. Offene Punkte

| # | Punkt | Einordnung |
|---|---|---|
| 1 | ~~**Heartbeat-Einstellung des Brokers ungeprüft.**~~ **Geschlossen am 13.11.2026.** `-rabbit heartbeat 300` in `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS` greift. L14 zeigt `300` für den `chat-service` und `60` für den `batch-writer`. Die `rabbitmq.conf` als Ausweg wird nicht gebraucht. | erledigt |
| 2 | **Clients der pausierten Instanz verlieren Nachrichten still.** Ihr Stream reisst nicht ab, also kommt kein `Last-Event-ID`. Was `drop-head` verworfen hat, fehlt ihnen. | Möglich: nach dem Fortsetzen alle Streams schliessen, damit die Browser nachliefern. Zurückgestellt. |
| 3 | **Restlücke durch vertauschtes Veröffentlichen.** `sentAt` wird vor dem Veröffentlichen gesetzt. Zwei gleichzeitige Requests können vertauscht am Exchange ankommen. Reisst die Verbindung genau dazwischen ab, liegt die zweite vor dem Anker und wird nie nachgeliefert. | Fenster im Mikrosekundenbereich. Hängt an `PLANUNG.md`, Punkt 3. |
| 4 | **1 s Verzögerung reicht nicht, wenn der `batch-writer` zurückstaut.** Dann fehlen Nachrichten, die vor der Registrierung veröffentlicht, aber noch nicht geschrieben wurden. | Faustregel, kein Beweis. |
| 5 | **Kein Nachliefern bei ausgefallener Datenbank** (S5 und S7 zugleich). | Stream läuft live weiter, die Lücke steht im Log. |
| 6 | **Ein langsamer Client blockiert alle.** Der Listener sendet synchron. Ein voller TCP-Puffer hält die Instanz auf, ihre Queue läuft über. | Abhilfe: begrenzte Warteschlange pro Client. Zurückgestellt. |
| 7 | **Viel Verkehr in einem Raum kann das Nachliefern immer wieder abbrechen.** Läuft `pending` dauernd über 1000, verbindet der Client endlos neu. | Bei realistischem Chatverkehr nicht zu erwarten, aber nicht ausgeschlossen. |
| 8 | **Unschärfe beim Löschen durch `x-expires`** ist nicht zugesagt, nur gemessen. **Beziffert am 13.11.2026:** fünf Läufe ergaben 11–13 s bei 10 s `x-expires`, die Unschärfe liegt also unter 3 s. Bleibt offen, weil RabbitMQ dafür keine Zusage macht — unter Last könnte es länger dauern. Der Abstand zum Limit von 30 s ist aber mehr als das Doppelte. | bleibt offen, quantifiziert |
| 9 | **Index ohne `id`.** Der Plan hängt von der Datenmenge ab (3.7.4). Eindeutig wäre ein Index `(room_id, sent_at, id)` als V2-Migration im `batch-writer`. **Entspannt am 13.11.2026:** Mit Daten in mehreren Räumen nutzt Postgres den vorhandenen Index in beiden gemessenen Fällen von selbst (L12, 0,3 und 0,6 ms). Der Punkt bleibt offen, weil die Planwahl weiterhin von der Verteilung abhängt — er ist aber kein Leistungsproblem. | Eigene Aufgabe, nicht in diesem Auftrag. |
| 10 | **Lesezugriff mit dem Schreib-Benutzer.** | Sauberer: eigener Benutzer nur zum Lesen. |
| 11 | **Gateway** muss `Last-Event-ID` durchreichen und darf SSE nicht puffern. | Kommt mit dem Gateway. |
| 12 | **Publisher Confirms fehlen weiterhin.** | Offener Punkt aus Bewertung 1. |
