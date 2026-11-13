package ch.benedict.m321.chatservice.stream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Merkt sich, welche offenen SSE-Streams zu welchem Raum gehören.
 *
 * Nötig, weil der Fanout JEDER Instanz JEDE Nachricht gibt. Beim Binden
 * liesse sich das nicht filtern: Ein Fanout ignoriert den Routing-Key, und
 * die roomId steht nur im Körper der Nachricht. Gefiltert wird deshalb erst
 * nach dem Konsumieren, und dieses Register ist die Nachschlagetabelle dafür
 * (Spezifikation 3.1).
 *
 * Von vier Seiten gleichzeitig benutzt: Der Request-Thread trägt beim GET
 * ein, der Listener-Thread liest beim Zustellen, der Ping-Job läuft über
 * alle, und ab Schritt 7 kommen die Nachliefer-Threads dazu. Darum eine
 * ConcurrentHashMap und je Raum eine nebenläufigkeitsfeste Menge — eine
 * gewöhnliche HashMap würde unter gleichzeitigen Zugriffen stillschweigend
 * Einträge verlieren.
 */
@Slf4j
@Component
public class StreamRegistry {

    /**
     * Raum-Kennung auf die offenen Streams dieses Raums.
     *
     * Der Zustand liegt bewusst nur im Speicher dieser Instanz. Eine
     * Verbindung gehört genau einem Prozess; sie in einer Datenbank zu
     * halten, hülfe niemandem — kein anderer Prozess könnte in sie
     * schreiben.
     */
    private final ConcurrentHashMap<UUID, Set<StreamConnection>> connectionsByRoom = new ConcurrentHashMap<>();

    /**
     * Trägt eine neue Verbindung ein.
     *
     * computeIfAbsent statt "nachsehen, dann anlegen": Zwei Clients, die sich
     * im selben Augenblick für denselben Raum anmelden, würden sonst zwei
     * Mengen anlegen, und eine der beiden Verbindungen wäre verloren.
     */
    public void register(StreamConnection connection) {
        UUID roomId = connection.roomId();
        Set<StreamConnection> connections = connectionsByRoom.computeIfAbsent(roomId, key -> ConcurrentHashMap.newKeySet());
        connections.add(connection);
        log.debug("Stream registered for room {}, {} open now", roomId, connections.size());
    }

    /**
     * Trägt eine Verbindung wieder aus.
     *
     * Die leere Menge eines Raums bleibt danach stehen. Sie aufzuräumen
     * bräuchte eine Sperre gegen den Fall, dass sich im selben Augenblick
     * jemand neu anmeldet — und das kostet mehr als ein leerer Eintrag pro
     * Raum, der je einen Zuhörer hatte.
     *
     * Mehrfaches Austragen derselben Verbindung ist harmlos. Das ist
     * Absicht: In Schritt 3 melden drei Callbacks dasselbe Ereignis, und der
     * Listener trägt bei einem fehlgeschlagenen Senden ebenfalls aus.
     */
    public void unregister(StreamConnection connection) {
        UUID roomId = connection.roomId();
        Set<StreamConnection> connections = connectionsByRoom.get(roomId);
        if (connections == null) {
            return;
        }
        connections.remove(connection);
        log.debug("Stream unregistered for room {}, {} open now", roomId, connections.size());
    }

    /**
     * Die offenen Streams eines Raums.
     *
     * Gibt die lebende Menge zurück, keine Kopie. Über sie zu laufen ist
     * erlaubt, während andere Threads eintragen oder austragen — die Menge
     * aus ConcurrentHashMap wirft dabei keine
     * ConcurrentModificationException. Der Preis: Wer gerade dazukommt, wird
     * vielleicht noch nicht gesehen. Für eine Chat-Nachricht ist das
     * belanglos, denn genau dafür gibt es das Nachliefern aus Schritt 7.
     */
    public Set<StreamConnection> connectionsForRoom(UUID roomId) {
        Set<StreamConnection> connections = connectionsByRoom.get(roomId);
        if (connections == null) {
            return Collections.emptySet();
        }
        return connections;
    }

    /**
     * Alle offenen Streams dieser Instanz, über alle Räume.
     *
     * Gibt eine KOPIE zurück, anders als connectionsForRoom. Die beiden
     * Aufrufer — der Ping und das Herunterfahren — tragen beim Durchlaufen
     * Verbindungen aus: Der Ping, wenn ein Client tot ist, das
     * Herunterfahren bei jeder. Über die lebende Menge zu laufen und sie
     * dabei zu verändern wäre erlaubt, aber nicht mehr nachvollziehbar.
     */
    public List<StreamConnection> allConnections() {
        List<StreamConnection> allConnections = new ArrayList<>();
        Collection<Set<StreamConnection>> connectionsPerRoom = connectionsByRoom.values();
        for (Set<StreamConnection> connections : connectionsPerRoom) {
            allConnections.addAll(connections);
        }
        return allConnections;
    }
}
