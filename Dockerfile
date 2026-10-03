# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------- build stage
# Debian-based (not Alpine): protoc / protoc-gen-grpc-java are glibc binaries.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B --no-transfer-progress -DskipTests clean package

# ---------------------------------------------------------------- shared runtime base
FROM eclipse-temurin:17-jre AS runtime-base
RUN groupadd --system gfs && useradd --system --gid gfs --no-create-home gfs
WORKDIR /app

# ---------------------------------------------------------------- master
FROM runtime-base AS master
COPY --from=build /workspace/gfs-master/target/gfs-master-*.jar /app/app.jar
USER gfs
EXPOSE 9000
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]

# ---------------------------------------------------------------- chunkserver
FROM runtime-base AS chunkserver
COPY --from=build /workspace/gfs-chunkserver/target/gfs-chunkserver-*.jar /app/app.jar
# Create + chown BEFORE declaring the volume so named volumes inherit gfs ownership.
RUN mkdir /data && chown gfs:gfs /data
ENV GFS_DATA_DIR=/data
VOLUME /data
USER gfs
EXPOSE 9101 9102 9103
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]

# ---------------------------------------------------------------- client (CLI)
FROM runtime-base AS client
COPY --from=build /workspace/gfs-client/target/gfs-client-*.jar /app/app.jar
USER gfs
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]