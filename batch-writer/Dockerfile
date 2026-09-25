# Stufe 1: bauen
# Der Build-Kontext ist das Projekt-Wurzelverzeichnis, weil das Modul
# das Eltern-POM braucht.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
# Maven muss JEDES Modul lesen koennen, das im Eltern-POM steht, sonst
# bricht es mit "Child module does not exist" ab. Die Quellen der
# anderen Module braucht es nicht — nur ihre pom.xml.
COPY chat-service/pom.xml chat-service/pom.xml
COPY batch-writer/pom.xml batch-writer/pom.xml
COPY batch-writer/src batch-writer/src
# Tests werden hier übersprungen: Testcontainers bräuchte einen Docker-Daemon
# INNERHALB des Builds. Getestet wird vorher mit "mvn test".
RUN mvn -q -pl batch-writer -am package -DskipTests

# Stufe 2: laufen
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/batch-writer/target/batch-writer-0.1.0-SNAPSHOT.jar app.jar
# Kein EXPOSE: dieser Dienst hat keine Schnittstelle, auf der jemand
# anklopfen könnte. Er holt sich seine Arbeit selbst aus der Queue.
ENTRYPOINT ["java", "-jar", "app.jar"]
