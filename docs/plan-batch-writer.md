# batch-writer — Umsetzungsplan

**Ziel:** Der `batch-writer` holt Nachrichten aus der Queue `chat.persist` und legt sie
gebündelt in PostgreSQL ab. Er ist der einzige Dienst, der in die Chat-Datenbank schreibt.

**Spezifikation:** [`spec-batch-writer.md`](spec-batch-writer.md) — dort stehen Vertrag,
Verhalten, Datenmodell und Abnahmekriterien. Dieser Plan sagt nur, in welcher Reihenfolge
gebaut wird.

**Vorbild für Aufbau und Detailtiefe:** [`plan-chat-service.md`](plan-chat-service.md).

**Architektur:** `listener → repository`, dazu `config` für alles, was beim Start
eingerichtet wird, und `dto` für die eigene Kopie des Queue-Vertrags. Der Listener kennt
kein SQL, das Repository kennt kein RabbitMQ.

**Tech-Stack:** Java 21, Spring Boot 3.5.16, Spring AMQP, Spring JDBC (`JdbcTemplate`),
Spring Retry, Flyway, PostgreSQL 16, RabbitMQ 3.13, JUnit 5, Testcontainers.

---

## Globale Vorgaben

Gelten für **jede** Aufgabe in diesem Plan:

- **Code auf Englisch**, **alles andere auf Deutsch** (`CLAUDE.md`).
- **Keine verschachtelten Aufrufe**, ein Ergebnis pro Zeile in eine benannte Variable.
  Gilt auch in Tests.
- **Keine Streams** — `for`-Schleifen, die man laut vorlesen kann.
- **Kommentar über jeder Klasse und jeder Methode**, der das *Warum* erklärt.
- **Kein JPA.** `JdbcTemplate.batchUpdate` ist der Punkt, den dieser Dienst zeigen soll
  (`PLANUNG.md` 2.1).
- **Kein `ports:`-Eintrag** in `docker-compose.yml`.
- **Keine Geheimnisse im Repository**, nur `.env.example`.
- **Ein Thema pro Commit**, Commit-Message auf Deutsch.
- Jeder Commit endet mit:
  ```
  Co-Authored-By: Claude Opus 5 <claude@pritz-it.com>
  ```

---

## Reihenfolge und warum

Zuerst die **Datenbankseite**, dann die **Queue-Seite**, dann die **Fehlerfälle**, zuletzt
die **Einbettung in den Stack**.

Grund: Die Datenbankseite lässt sich mit einem einzigen Testcontainer prüfen, ohne dass
eine Nachricht im Spiel ist. Wäre die Queue zuerst dran, müsste jeder Test schon beide
Container starten, und bei einem Fehler wüsste man nicht, welche Seite schuld ist. Die
Fehlerfälle kommen nach dem Normalfall, weil man erst etwas kaputt machen kann, das
funktioniert.

---

## Aufgaben

### 1 · Modul anlegen

- [ ] `batch-writer/pom.xml` mit Eltern-POM, Modul in `pom.xml` eintragen
- [ ] `BatchWriterApplication.java`
- [ ] `application.yml` mit RabbitMQ- und Datenquellen-Einstellungen aus Umgebungsvariablen
- [ ] `Dockerfile` nach dem Vorbild des `chat-service` (zweistufig, Tests übersprungen)

**Test:** `BatchWriterApplicationTests` — der Spring-Kontext startet.
**Warum zuerst:** Ohne Modul gibt es nichts, wohin der Code kommt. Der Kontexttest fängt
Tippfehler in `application.yml` sofort ab.

### 2 · Postgres in den Stack

- [ ] Dienst `postgres` in `docker-compose.yml`: Image `postgres:16`, Healthcheck, Volume,
      **kein** `ports:`
- [ ] `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` in `.env.example`

**Test:** `docker compose up -d postgres` und `docker compose ps` zeigt `healthy`.
**Warum hier:** Die Datenbank muss stehen, bevor jemand ein Schema hineinlegt. Der
`batch-writer` selbst kommt erst in Aufgabe 9 dazu — vorher hätte er nichts zu tun.

### 3 · Schema mit Flyway

- [ ] `V1__create_message_table.sql`: Tabelle `message` und Index nach `PLANUNG.md` 3.7
- [ ] Flyway-Abhängigkeit und Einstellungen

**Test:** `MessageSchemaTest` mit Testcontainers-Postgres — nach dem Start existiert die
Tabelle mit allen sechs Spalten.
**Warum vor dem Repository:** Ohne Tabelle lässt sich kein INSERT testen.

### 4 · Repository mit Stapel-INSERT

- [ ] `MessageRow` — was in einer Tabellenzeile steht
- [ ] `MessageRepository.insertBatch(List<MessageRow>)` mit
      `JdbcTemplate.batchUpdate` und `ON CONFLICT (id) DO NOTHING`, `@Transactional`

**Test:** `MessageRepositoryTest` — (a) zehn Zeilen werden geschrieben, (b) dieselbe ID
zweimal ergibt genau eine Zeile.
**Warum jetzt:** Das ist der Kern des Dienstes. Er ist ohne Queue prüfbar, also wird er
ohne Queue geprüft.

### 5 · Queue-Vertrag und Konverter

- [ ] `ChatMessage` — **eigene** Kopie des Records, bewusst nicht geteilt
- [ ] `QueueNames` — eigene Kopie der drei Namen
- [ ] `RabbitConfig`: JSON-Konverter, Queue und DLQ deklarieren, Listener-Factory mit
      Stapelbetrieb (500 Stück / 200 ms / `prefetch` 500)

**Test:** `ChatMessageConversionTest` — ein JSON ohne `__TypeId__`, nur mit
`content_type: application/json`, wird zu einem `ChatMessage`.
**Warum getrennt vom Listener:** Genau hier entscheidet sich Szenario S5. Der Test zeigt
es ohne Broker und ohne Datenbank.

### 6 · Listener: von der Queue in die Tabelle

- [ ] `MessageBatchListener` mit `@RabbitListener` auf `chat.persist`, Parameter
      `List<ChatMessage>`
- [ ] Umwandlung `ChatMessage` → `MessageRow`, dann `insertBatch`

**Test:** `MessageBatchListenerIntegrationTest` (**T-3**) — echte Queue und echte
Datenbank über Testcontainers: 50 Nachrichten gesendet, 50 Zeilen in der Tabelle, Queue
leer.
**Warum jetzt:** Beide Enden funktionieren; dies ist die Naht dazwischen.

### 7 · Duplikat über die Queue

- [ ] Kein neuer Code erwartet — Aufgabe 4 und 5 sollten reichen

**Test:** `DuplicateMessageIntegrationTest` (**T-5**) — dieselbe Nachricht zweimal roh in
`chat.persist` gelegt, nur mit `content_type: application/json`. Erwartet: eine Zeile,
`chat.dlq` bleibt leer.
**Warum ein eigener Schritt:** Szenario S5 ist drei Punkte wert und prüft zwei Dinge auf
einmal (Duplikat **und** fehlender Typ-Header). Wenn hier doch Code nötig wird, sieht man
im Git-Log genau, welcher.

### 8 · Datenbankausfall aushalten

- [ ] Spring Retry auf dem Schreibweg: 20 Versuche, 1 s bis 5 s, nur bei
      `DataAccessException`
- [ ] Nach Erschöpfung: Ausnahme weiterreichen, damit der Stapel wieder in die Queue geht

**Test:** `DatabaseOutageTest` (**T-7**) — ein Repository, das die ersten Versuche
scheitern lässt und danach gelingt. Erwartet: die Zeilen sind am Ende geschrieben, ohne
dass jemand etwas neu startet.
**Warum nach dem Normalfall:** Man kann nur wiederholen, was im Normalfall funktioniert.

### 9 · Unlesbare Nachrichten abweisen

- [ ] Pflichtfelder prüfen; bei `null` oder ungültigem JSON
      `AmqpRejectAndDontRequeueException`, damit die Nachricht in `chat.dlq` landet

**Test:** `InvalidMessageIntegrationTest` — kaputtes JSON in die Queue, erwartet: keine
Zeile in `message`, eine Nachricht in `chat.dlq`, der Dienst läuft weiter.
**Warum getrennt von Aufgabe 8:** Der Unterschied ist der Kern: Bei Aufgabe 8 ist der
**Empfänger** kaputt und wir versuchen es erneut. Hier ist die **Nachricht** kaputt und
Wiederholen hilft nie.

### 10 · In den Stack einhängen

- [ ] Dienst `batch-writer` in `docker-compose.yml`, `depends_on` auf `postgres`
      (`service_healthy`) und `rabbitmq` (`service_healthy`), **kein** `ports:`
- [ ] Tabelle „Stand" im `README.md` nachführen

**Test:** Abnahmekriterien A2 bis A9 aus der Spezifikation, von Hand am laufenden Stack.
**Warum zuletzt:** Erst jetzt gibt es einen Dienst, der beim Starten auch etwas tut.

---

## Reihenfolge der Tests zu den Szenarien

| Szenario | Deckt ab | Aufgabe |
|---|---|---|
| S1 `mvn clean test` grün | alle Tests | 1–9 |
| S2 Stack startet, kein Port | Compose | 2, 10 |
| S3 1000 Nachrichten in der DB | Listener + Repository | 4, 6 |
| S4 Writer gestoppt, nichts verloren | durable Queue + ACK nach COMMIT | 6 |
| S5 Duplikat, kein `__TypeId__` | `ON CONFLICT` + Typ-Ableitung | 5, 7 |
| S6 Zwei Instanzen | Competing Consumers, kein Zustand im Speicher | 6, 10 |
| S7 Datenbank weg | Spring Retry | 8 |
| S8 Regeln aus `CLAUDE.md` | gilt in jeder Aufgabe | 1–10 |
