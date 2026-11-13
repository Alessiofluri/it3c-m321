package ch.benedict.m321.chatservice.stream;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

/**
 * Ein offener SSE-Stream und der Raum, für den er sich interessiert.
 *
 * Mehr braucht es in diesem Schritt nicht. Zustand, Warteschlange und
 * Sperre für das Nachliefern kommen in Schritt 7 dazu — dann wird daraus
 * eine Klasse. Solange nur zwei unveränderliche Werte darin stehen, ist ein
 * record die kürzeste ehrliche Form.
 *
 * Wichtig für das Register: Ein record vergleicht sich über seine Felder,
 * und SseEmitter vergleicht sich über die Objektidentität. Zwei Verbindungen
 * sind also nur dann gleich, wenn es wirklich dieselbe ist — genau das
 * braucht eine Menge, aus der einzelne Verbindungen wieder ausgetragen
 * werden.
 */
public record StreamConnection(
        SseEmitter emitter,
        UUID roomId) {
}
