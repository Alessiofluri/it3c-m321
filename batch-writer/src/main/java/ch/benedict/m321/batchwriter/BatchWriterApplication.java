package ch.benedict.m321.batchwriter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

/**
 * Startpunkt des batch-writer.
 *
 * Der Dienst hat keine HTTP-Schnittstelle. Er horcht an der Queue
 * chat.persist und schreibt, was dort ankommt, gebuendelt in PostgreSQL.
 * Er ist der einzige Dienst, der in die Chat-Datenbank schreibt.
 *
 * @EnableRetry schaltet die Wiederholversuche im MessageRepository ein.
 * Ohne diese Zeile wuerde die Annotation dort stillschweigend nichts
 * tun — und der Ausfall der Datenbank waere sofort ein Fehler.
 */
@SpringBootApplication
@EnableRetry
public class BatchWriterApplication {

    /** Startet die Spring-Anwendung. */
    public static void main(String[] args) {
        SpringApplication.run(BatchWriterApplication.class, args);
    }
}
