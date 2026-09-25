package ch.benedict.m321.batchwriter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Startpunkt des batch-writer.
 *
 * Der Dienst hat keine HTTP-Schnittstelle. Er horcht an der Queue
 * chat.persist und schreibt, was dort ankommt, gebuendelt in PostgreSQL.
 * Er ist der einzige Dienst, der in die Chat-Datenbank schreibt.
 */
@SpringBootApplication
public class BatchWriterApplication {

    /** Startet die Spring-Anwendung. */
    public static void main(String[] args) {
        SpringApplication.run(BatchWriterApplication.class, args);
    }
}
