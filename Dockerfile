# docker build --build-arg MODULE=trip-service -t ridehail/trip-service:1.0.0 .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
ARG MODULE
RUN mvn -q -B -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/*.jar app.jar
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75","-jar","/app/app.jar"]
