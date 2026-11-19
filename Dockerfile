FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY .mvn/wrapper/maven-wrapper.properties .mvn/wrapper/
COPY mvnw pom.xml ./
COPY src/main src/main
RUN sh ./mvnw -B -ntp -DskipTests package

FROM eclipse-temurin:21.0.9_10-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build --chown=10001:10001 /build/target/ms-kafka-admin-0.0.1-SNAPSHOT.jar app.jar
USER 10001:10001
EXPOSE 8089
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-jar","/app/app.jar"]
