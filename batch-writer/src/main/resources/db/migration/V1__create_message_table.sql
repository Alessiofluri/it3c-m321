-- Die Tabelle, in die der batch-writer schreibt.
-- Spalten exakt nach PLANUNG.md Abschnitt 3.7.
CREATE TABLE message (
    -- Die UUID kommt vom chat-service, nicht von der Datenbank.
    -- Genau deshalb kann ON CONFLICT DO NOTHING ein Duplikat erkennen.
    id          uuid         PRIMARY KEY,
    room_id     uuid         NOT NULL,
    -- sub-Kennung aus Keycloak. Benutzer verwalten wir nicht selbst.
    sender_id   varchar(255) NOT NULL,
    -- Bewusst denormalisiert: die Historie soll lesbar bleiben, auch
    -- wenn ein Konto spaeter geloescht wird.
    sender_name varchar(255) NOT NULL,
    content     text         NOT NULL,
    -- Mit Zeitzone, weil der Zeitstempel als UTC-Instant ankommt.
    sent_at     timestamptz  NOT NULL
);

-- Die einzige Abfrage im Lesepfad ist "die letzten 50 Nachrichten eines
-- Raums". Der batch-writer liest selbst nie, der Index gehoert aber zur
-- Tabelle und wird mit ihr angelegt.
CREATE INDEX idx_message_room_sent_at ON message (room_id, sent_at DESC);
