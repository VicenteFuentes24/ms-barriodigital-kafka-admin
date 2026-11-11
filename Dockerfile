FROM eclipse-temurin:21.0.9_10-jre-jammy
WORKDIR /app
COPY --chown=10001:10001 target/ms-kafka-admin-0.0.1-SNAPSHOT.jar app.jar
USER 10001:10001
EXPOSE 8089
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-jar","/app/app.jar"]
