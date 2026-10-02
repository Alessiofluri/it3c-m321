# Spezifikation: batch-writer

**Modul M321 · Klasse IT3c · Bewertung 1 · 25.09.2026**

Der `batch-writer` ist der einzige Dienst, der in die Chat-Datenbank schreibt. Er holt
Nachrichten aus der Queue `chat.persist` und legt sie gebündelt in PostgreSQL ab.

Grundlage: [`../PLANUNG.md`](../PLANUNG.md), Abschnitte 3.4 bis 3.7 und Schritt 4 der
Umsetzungsreihenfolge.

---

## 1. Zweck und Abgrenzung

### Was der Dienst tut

Er nimmt Nachrichten aus `chat.persist` entgegen, sammelt sie zu einem Stapel und schreibt
den Stapel mit **einem** JDBC-Batch in die Tabelle `message`. Erst wenn die Datenbank den
Stapel bestätigt hat, bestätigt er ihn gegenüber RabbitMQ.

**Warum es ihn gibt:** `PLANUNG.md` Abschnitt 4.1 rechnet mit 1'667 Nachrichten pro
Sekunde. So viele einzelne Transaktionen hält keine Datenbank dauerhaft aus. Bei einer
Stapelgrösse von 500 werden daraus rund 3,3 Schreibvorgänge pro Sekunde — Faktor 500
weniger Transaktionen. Die Queue ist damit kein Transportweg, sondern ein **Puffer**.

### Was der Dienst bewusst nicht tut

| Nicht enthalten | Warum |
|---|---|
| Chat-Historie lesen (`GET /rooms/{id}/messages`) | Laut `PLANUNG.md` 3.1 liest der `chat-service` die Historie. Der `batch-writer` ist reiner Schreiber. |
| Räume und Mitgliedschaften (`room`, `room_member`) | Nicht Teil von Bewertung 1. Die Tabelle `message` steht für sich; sie bekommt **keinen** Fremdschlüssel auf `room` (siehe Abschnitt 4.2). |
| Keycloak, Token-Prüfung | Der Dienst ist von aussen nicht erreichbar und konsumiert nur aus dem internen Netz. |
| HTTP-Schnittstelle | Er hat keine. Seine einzige Eingabe ist die Queue. |
| `chat.delivery` konsumieren | Das ist Sache des `web-gateway`. |
| Exactly-once-Zustellung | Wird nicht behauptet. Siehe Abschnitt 3.3. |

---

## 2. Vertrag: was auf der Queue ankommt

### 2.1 Woher ich das weiss

Nicht geraten, sondern aus dem vorhandenen Code des `chat-service` gelesen:

| Frage | Beleg im Repository |
|---|---|
| Welche Felder? | `chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java` |
| Welche Queue? | `chat-service/.../config/QueueNames.java` → `PERSIST_QUEUE = "chat.persist"` |
| Wie wird gesendet? | `chat-service/.../service/MessagePublisher.java` → `convertAndSend(PERSIST_QUEUE, message)` |
| Welches Format? | `chat-service/.../config/RabbitConfig.java` → `Jackson2JsonMessageConverter` mit dem `ObjectMapper` von Spring Boot |
| Queue-Eigenschaften? | `RabbitConfig.persistQueue()` → `durable`, Dead-Letter-Exchange `""`, Routing-Key `chat.dlq` |

### 2.2 Das JSON

```json
{
  "id": "3f120a73-1922-4577-ae9a-d28063636992",
  "roomId": "39acf48b-5d49-40f0-a5d6-cd5bfdb59385",
  "senderId": "auth0|42",
  "senderName": "Alessio",
  "content": "Hallo",
  "sentAt": "2026-09-25T13:48:35.746Z"
}
```

| Feld | JSON-Typ | Herkunft | Nullbar |
|---|---|---|---|
| `id` | String (UUID) | vom `chat-service` vergeben, nicht vom Client | nein |
| `roomId` | String (UUID) | aus dem Request | nein |
| `senderId` | String | `sub` aus Keycloak, später | nein |
| `senderName` | String | Anzeigename, bewusst denormalisiert | nein |
| `content` | String | Text der Nachricht | nein |
| `sentAt` | String (ISO-8601, UTC) | Server-Zeitstempel | nein |

Alle sechs Felder sind im `chat-service` als `@NotNull`/`@NotBlank` validiert, bevor die
Nachricht die Queue erreicht (`SendMessageRequest`). Der `batch-writer` prüft sie trotzdem
noch einmal — siehe Abschnitt 3.4, Fehlerfall „unvollständige Nachricht".

### 2.3 Die Kopfzeilen

Spring AMQP setzt beim Senden automatisch:

| Header | Wert | Bedeutung für uns |
|---|---|---|
| `content_type` | `application/json` | **Das einzige, worauf wir uns verlassen.** |
| `content_encoding` | `UTF-8` | |
| `__TypeId__` | `ch.benedict.m321.chatservice.dto.ChatMessage` | **Wird bewusst ignoriert.** |
| `delivery_mode` | `2` (persistent) | Die Nachricht überlebt einen Broker-Neustart. Nachgemessen am 02.10.2026: fünf Nachrichten lagen vor und nach `docker compose restart rabbitmq` unverändert in `chat.persist`. |

**Die wichtigste Entscheidung des ganzen Vertrags:** Der `batch-writer` darf sich **nicht**
auf `__TypeId__` verlassen.

Der Header nennt eine Klasse aus dem `chat-service`. Die gibt es im `batch-writer` gar
nicht — er hat seine **eigene** Kopie des Records. Das ist so gewollt, `ChatMessage.java`
sagt es selbst: *„der Vertrag ist das JSON, nicht diese Klasse. Ein gemeinsames Modul würde
alle Dienste aneinanderbinden."*

Deshalb wird der Typ aus der **Signatur der Listener-Methode** abgeleitet (Spring AMQP:
`TypePrecedence.INFERRED`), nicht aus dem Header. Zwei Konsequenzen:

1. Eine Nachricht mit `__TypeId__` aus dem `chat-service` funktioniert.
2. Eine Nachricht **ohne** `__TypeId__`, nur mit `content_type: application/json`,
   funktioniert genauso — genau das prüft Szenario **S5**.

Belegt wird das durch Test `T-5` (Abschnitt 5), nicht durch diese Behauptung.

---

## 3. Verhalten

### 3.1 Normalfall

```
chat.persist ──► Stapel sammeln ──► ein JDBC-Batch ──► COMMIT ──► ACK für den Stapel
                 (500 Stück ODER
                  200 ms, was zuerst kommt)
```

1. Der Consumer holt Nachrichten mit `prefetch = 500` vom Broker.
2. Er sammelt, bis **500 Nachrichten** beisammen sind **oder 200 ms** vergangen sind.
   Beide Werte stammen aus `PLANUNG.md` 3.6.
3. Der Stapel geht in **einer** Transaktion als `JdbcTemplate.batchUpdate` an Postgres.
4. Nach dem COMMIT bestätigt der Container den **ganzen Stapel** mit einem ACK.

**Warum 500 und 200 ms zusammen?** 500 allein würde bei wenig Betrieb bedeuten, dass eine
einzelne Nachricht liegen bleibt, bis 499 weitere kommen. Die 200 ms sind die Obergrenze
für die Verzögerung, die ein Nutzer im schlimmsten Fall wartet, bis seine Nachricht
dauerhaft gespeichert ist. Zugestellt wurde sie da längst — Zustellung und Speicherung sind
entkoppelt (`PLANUNG.md` 3.4).

**Warum erst COMMIT, dann ACK?** Umgekehrt wäre die Nachricht aus der Queue verschwunden,
bevor sie sicher in der Datenbank steht. Stürzt der Dienst dazwischen ab, wäre sie weg.

### 3.2 Zustellgarantie: At-least-once

Bestätigt wird erst nach dem COMMIT. Stirbt der Dienst mitten im Stapel, liefert RabbitMQ
alles erneut. **Nichts geht verloren, aber Duplikate sind möglich.**

Wir behaupten ausdrücklich **kein** Exactly-once. Stattdessen machen wir Duplikate
harmlos — siehe 3.3.

### 3.3 Fehlerfall: dieselbe Nachricht kommt zweimal (S5)

**Verhalten:** Die zweite Einfügung wird verworfen. In der Tabelle steht genau eine Zeile.
Nichts landet in `chat.dlq`. Die Nachricht wird normal bestätigt.

**Wie:** `message.id` ist Primärschlüssel, das INSERT endet auf `ON CONFLICT (id) DO NOTHING`.

**Warum so und nicht anders:**

| Verworfene Variante | Grund |
|---|---|
| Vorher `SELECT`, ob die ID existiert | Ein zusätzlicher Roundtrip pro Nachricht — genau das, was der Stapel vermeiden soll. Ausserdem eine Race Condition zwischen zwei Instanzen. |
| `ON CONFLICT DO UPDATE` | Würde eine bestehende Nachricht überschreiben. Eine einmal gespeicherte Nachricht ist unveränderlich. |
| Duplikate im Speicher merken | Überlebt keinen Neustart und funktioniert bei zwei Instanzen nicht. |

Die Prüfung liegt damit in der Datenbank, also an der einzigen Stelle, die alle Instanzen
gemeinsam sehen.

### 3.4 Fehlerfall: unlesbare oder unvollständige Nachricht

Hier sind **zwei** Fälle zu unterscheiden. Sie verhalten sich verschieden, und der
Unterschied ist keine Feinheit:

| Fall | Was passiert | Folge für den Rest des Stapels |
|---|---|---|
| **Pflichtfeld fehlt**, JSON ist aber lesbar | Der Listener erkennt es selbst und legt genau diese Nachricht nach `chat.dlq` | Die übrigen Nachrichten werden normal geschrieben |
| **JSON ist gar nicht lesbar** | Der Konverter scheitert, bevor unser Code läuft. Spring lehnt den **ganzen Stapel** ohne Requeue ab, er geht nach `chat.dlq` | Auch gesunde Nachrichten desselben Stapels landen dort |

**Warum wir den ersten Fall selbst behandeln:** Ohne die Prüfung im Listener würde der
Stapel erst am `NOT NULL` der Tabelle scheitern. Dann wäre die ganze Transaktion hinüber,
und alle gesunden Nachrichten desselben Stapels kämen erneut — immer wieder, weil die
kaputte Nachricht ja bei jedem Versuch wieder dabei wäre. Eine einzige unvollständige
Nachricht könnte damit den Schreibweg dauerhaft blockieren.

**Warum wir den zweiten Fall nicht auch auseinandernehmen:** Dazu müsste der Listener
rohe `Message`-Objekte entgegennehmen und jede einzeln selbst umwandeln. Das wäre
deutlich mehr Code für einen Fall, der im Betrieb nicht vorkommt: Der `chat-service` ist
der einzige Erzeuger, und er sendet immer gültiges JSON. Auftreten kann der Fall nur,
wenn jemand von Hand etwas in die Queue legt. Das ist bewusst in Kauf genommen und hier
benannt, statt es zu verschweigen.

**Warum in beiden Fällen kein erneuter Versuch?** Eine Nachricht, die sich nicht lesen
lässt, lässt sich auch beim zehnten Mal nicht lesen. Sie würde die Queue endlos
blockieren. Das ist der Unterschied zum Datenbankausfall in 3.5: dort ist der
**Empfänger** weg und kommt zurück, hier ist die **Nachricht** kaputt und bleibt es.

### 3.5 Fehlerfall: die Datenbank ist weg (S7)

**Verhalten:** Der Dienst wartet und versucht es erneut. Er stirbt nicht, er braucht keinen
Neustart von Hand, und er wirft nichts in die DLQ. Sobald Postgres zurück ist, wird der
Stapel geschrieben.

**Wie:** Der Schreibvorgang ist mit Spring Retry abgesichert:

| Einstellung | Wert | Begründung |
|---|---|---|
| Wiederholt bei | `DataAccessException` | Alle Datenbankfehler von Spring, inklusive „Verbindung weg". |
| Versuche | 20 | Deckt zusammen mit der Wartezeit rund 90 Sekunden ab. |
| Wartezeit | 1 s, verdoppelnd, höchstens 5 s | Am Anfang schnell (kurze Aussetzer), danach schonend für die Datenbank beim Hochfahren. |

Sind alle 20 Versuche erschöpft, wird die Ausnahme weitergereicht. Der Container lehnt den
Stapel **mit** Requeue ab, RabbitMQ stellt ihn später erneut zu. Auch dann geht nichts
verloren.

**Warum kein sofortiges Requeue statt Warten?** Ein Requeue in einer Schleife erzeugt
Last auf dem Broker und im Log, ohne etwas zu verbessern — die Datenbank ist ja weiter
weg. Warten ist das ehrlichere Verhalten.

**Warum nicht in die DLQ?** Die Datenbank kommt zurück. Eine Nachricht auf dem Abstellgleis
müsste jemand von Hand zurückholen.

### 3.6 Fehlerfall: zwei Instanzen gleichzeitig (S6)

**Verhalten:** Beide Instanzen hängen an derselben Queue `chat.persist`. RabbitMQ verteilt
reihum, jede Nachricht geht an genau eine Instanz. Keine Nachricht wird doppelt
geschrieben, keine geht verloren.

**Wie:** Nichts Besonderes — das ist **Competing Consumers** und kommt vom Broker. Wichtig
ist nur, was wir *nicht* tun:

- keine exklusive Queue (die würde die zweite Instanz aussperren),
- kein Zustand im Speicher, der zwischen den Instanzen abgeglichen werden müsste,
- keine Annahme, dass genau eine Instanz läuft.

Beim Start legen beide Instanzen das Datenbankschema an. Flyway sperrt dabei über die
Tabelle `flyway_schema_history`, sodass nur eine Instanz wirklich migriert und die andere
wartet.

**Das ist die Stelle, an der Skalierung im Unterricht messbar wird** (`PLANUNG.md` 4.2).

### 3.7 Fehlerfall: RabbitMQ ist weg

**Verhalten:** Spring AMQP baut die Verbindung selbstständig neu auf (Standardverhalten,
alle 5 Sekunden). Nicht bestätigte Nachrichten bleiben in der Queue, weil die Queue
`durable` ist und die Nachrichten `persistent` sind.

---

## 4. Datenmodell und Konfiguration

### 4.1 Tabelle `message`

Spalten exakt nach `PLANUNG.md` 3.7:

```sql
CREATE TABLE message (
    id          uuid        PRIMARY KEY,
    room_id     uuid        NOT NULL,
    sender_id   varchar(255) NOT NULL,
    sender_name varchar(255) NOT NULL,
    content     text        NOT NULL,
    sent_at     timestamptz NOT NULL
);

CREATE INDEX idx_message_room_sent_at ON message (room_id, sent_at DESC);
```

### 4.2 Begründete Entscheidungen zum Schema

| Entscheidung | Begründung |
|---|---|
| `id` als Primärschlüssel, nicht als `serial` | Die UUID kommt vom `chat-service`. Genau sie macht `ON CONFLICT DO NOTHING` möglich und damit Duplikate harmlos. Ein eigener Zähler würde jede Wiederholung zu einer neuen Zeile machen. |
| **Kein** Fremdschlüssel auf `room` | Die Tabelle `room` gibt es noch nicht (nicht Teil dieser Bewertung). Ein Fremdschlüssel würde ausserdem bei jedem INSERT eine Prüfung auslösen — im Schreibpfad, den wir gerade schnell halten wollen. |
| `sender_name` mitgespeichert | Bewusst denormalisiert, `PLANUNG.md` 3.7: Die Historie soll lesbar bleiben, auch wenn ein Keycloak-Konto später gelöscht wird. |
| `timestamptz`, nicht `timestamp` | Der Zeitstempel kommt als UTC-`Instant`. Ohne Zeitzone wäre er beim Lesen mehrdeutig. |
| Index auf `(room_id, sent_at DESC)` | Die einzige Abfrage im Lesepfad ist „die letzten 50 Nachrichten eines Raums" (`PLANUNG.md` 3.7). Der Index gehört zur Tabelle, auch wenn der `batch-writer` selbst nie liest. |
| Schema kommt per **Flyway aus dem `batch-writer`** | Der Dienst, der die Tabelle schreibt, bringt sie mit. Ein Init-Skript im Postgres-Container würde nur beim allerersten Start eines leeren Volumes laufen — eine spätere Änderung käme nie an. Flyway läuft bei jedem Start und ist versioniert. |

### 4.3 Umgebungsvariablen

| Variable | Beispielwert in `.env.example` | Wer braucht sie | Bedeutung |
|---|---|---|---|
| `RABBITMQ_USER` | `chat` | rabbitmq, chat-service, batch-writer | Broker-Benutzer |
| `RABBITMQ_PASSWORD` | `bitte-lokal-aendern` | dieselben | Broker-Passwort |
| `POSTGRES_USER` | `chat` | postgres, batch-writer | Datenbank-Benutzer |
| `POSTGRES_PASSWORD` | `bitte-lokal-aendern` | dieselben | Datenbank-Passwort |
| `POSTGRES_DB` | `chat` | dieselben | Name der Datenbank |

Im Container gesetzte Werte ohne eigene Variable, weil sie sich nie unterscheiden:
`RABBITMQ_HOST=rabbitmq`, `POSTGRES_HOST=postgres`.

Die echte `.env` steht in `.gitignore`. Im Repository liegt nur `.env.example`.

### 4.4 Feste Einstellungen im Dienst

| Einstellung | Wert | Herkunft |
|---|---|---|
| Stapelgrösse | 500 | `PLANUNG.md` 3.6 |
| Sammelzeit | 200 ms | `PLANUNG.md` 3.6 |
| `prefetch` | 500 | muss mindestens so gross sein wie der Stapel, sonst wird er nie voll |
| Wiederholversuche bei DB-Fehler | 20, 1 s bis 5 s | Abschnitt 3.5 |

---

## 5. Abnahmekriterien

Jedes Kriterium ist messbar, mit dem Befehl, der es misst. Die Befehle laufen **von innen**
über `docker compose exec`, weil kein Container einen Port veröffentlicht.

Abkürzung für die Zählung in der Datenbank:

```bash
docker compose exec -T postgres psql -U chat -d chat -tAc "SELECT count(*) FROM message;"
```

Abkürzung für die Queue-Tiefe:

```bash
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages --quiet
```

| Nr | Kriterium | Messung | Erwartet |
|---|---|---|---|
| **A1** | Alle Tests grün | `mvn clean test` im Wurzelverzeichnis | ein Lauf, `BUILD SUCCESS` |
| **A2** | Stack startet aus frischem Klon | `cp .env.example .env && docker compose up -d --build`, dann `docker compose ps` | alle Dienste `running`, `postgres` `healthy` |
| **A3** | Kein Port veröffentlicht | `docker compose ps --format "{{.Service}}: {{.Publishers}}"` | bei jedem Dienst keine Host-Portnummer |
| **A4** | 1000 Nachrichten landen in der DB | 1000-mal `POST /messages` (Skript unten), dann Zählung | nach ≤ 60 s: `1000`, Queue `chat.persist` auf `0` |
| **A5** | Nichts geht verloren, wenn der Writer steht | `docker compose stop batch-writer`, 1000 senden, `docker compose start batch-writer` | nach dem Start: `1000`; höchstens 100 Transaktionen (Messung siehe A6) |
| **A6** | Wenige Transaktionen | `SELECT xact_commit FROM pg_stat_database WHERE datname='chat';` vor und nach A5 | Differenz ≤ 100 |
| **A7** | Duplikat erzeugt eine Zeile | dieselbe Nachricht zweimal direkt in `chat.persist` legen, nur mit `content_type: application/json` | genau `1` Zeile, `chat.dlq` bleibt `0` |
| **A8** | Zwei Instanzen stören sich nicht | `docker compose up -d --scale batch-writer=2`, 1000 senden, `rabbitmqctl list_consumers` | 2 Consumer auf `chat.persist`, `1000` Zeilen, keine Duplikate |
| **A9** | Datenbankausfall wird überstanden | `docker compose stop postgres`, 300 senden, nach 15 s `docker compose start postgres` | nach ≤ 90 s: `300` Zeilen, `batch-writer` lief durch (`docker compose ps` zeigt keinen Neustart) |
| **A10** | Regeln aus `CLAUDE.md` eingehalten | `grep -rn "\.stream()" batch-writer/src` und Sichtprüfung | keine Treffer; Kommentar über jeder Klasse und Methode; `.env` nicht im Repo |

### Nachrichten erzeugen

Der `batch-writer` veröffentlicht keinen Port, der `chat-service` auch nicht. Gesendet wird
deshalb aus einem Container im selben Netz:

```bash
docker compose exec -T chat-service sh -c '
  i=1
  while [ $i -le 1000 ]; do
    wget -qO- --header="Content-Type: application/json" \
      --post-data="{\"roomId\":\"39acf48b-5d49-40f0-a5d6-cd5bfdb59385\",\"senderId\":\"s1\",\"senderName\":\"Test\",\"content\":\"Nachricht $i\"}" \
      http://localhost:8080/messages > /dev/null
    i=$((i+1))
  done'
```

### Tests, die diese Kriterien absichern

| Test | Klasse | Sichert ab | Art |
|---|---|---|---|
| `T-3` | `MessageBatchListenerIntegrationTest` | A4 — Nachricht aus der Queue landet in der Tabelle | echte Queue + echte DB (Testcontainers) |
| `T-5` | `DuplicateMessageIntegrationTest` | A7 — Duplikat, und zwar **ohne** `__TypeId__`-Header | echte Queue + echte DB |
| `T-7` | `DatabaseOutageTest` | A9 — Datenbank fällt aus, Schreibversuch wird wiederholt | gezielter Test des Wiederholverhaltens |
| `T-9` | `IncompleteMessageIntegrationTest` | 3.4 — unvollständige Nachricht geht in die DLQ, der Rest des Stapels wird geschrieben | echte Queue + echte DB |

Dazu kommen vier Tests ohne eigene Kriteriumsnummer: `BatchWriterApplicationTests`
(Kontext startet), `MessageSchemaTest` (Flyway legt Tabelle und Index an),
`MessageRepositoryTest` (Stapel-INSERT und Duplikat auf Repository-Ebene) und
`ChatMessageConversionTest` (JSON ohne `__TypeId__` wird umgewandelt).

---

## 6. Offene Punkte

Ehrlich benannt, nicht weggeschwiegen:

| # | Punkt | Stand |
|---|---|---|
| 1 | **Die Datenbank wächst um ~1,2 GB/Stunde** (`PLANUNG.md` 4.1). | Ungelöst, und in dieser Bewertung ausdrücklich nicht gefordert. Weg wäre Partitionierung nach Tag oder ein Aufräum-Job. |
| 2 | **Reihenfolge innerhalb eines Raums** ist nicht garantiert. | Offener Punkt 3 der `PLANUNG.md`. Für den `batch-writer` ohne Folgen: er schreibt, was kommt. Sortiert wird beim Lesen nach `sent_at`. |
| 3 | **20 Versuche über rund 90 Sekunden** sind an Szenario S7 geeicht. | Ein längerer Ausfall führt zum Requeue. Die Nachricht ist dann weiterhin sicher, wird aber erst später geschrieben. |
| 4 | **Publisher Confirms** im `chat-service` fehlen. | Bestätigt der Broker das Annehmen nicht, könnte eine Nachricht verloren gehen, bevor der `batch-writer` sie je sieht. Offener Punkt 9 der `PLANUNG.md`, ausserhalb dieses Dienstes. |
