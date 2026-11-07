FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/notification-service-*-exec.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
