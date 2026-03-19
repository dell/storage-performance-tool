ARG BASE_VERSION

FROM eclipse-temurin:21-alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar \
    && ls -l build/libs

FROM ghcr.io/dell/storage-performance-tool:${BASE_VERSION}
ARG BASE_VERSION
ARG STORAGE_DRIVER_COOP_VERSION
ARG VERSION
COPY --from=0 /build/libs/spt-storage-driver-nio-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_nio.sh /opt/spt/entrypoint_storage_driver_nio.sh
RUN mv -f /tmp/spt-storage-driver-nio-*.jar /opt/spt/ext/ \
    && chmod +x /opt/spt/entrypoint_storage_driver_nio.sh \
    && curl http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/spt-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o /opt/spt/ext/spt-storage-driver-coop.jar
ENTRYPOINT ["/opt/spt/entrypoint_storage_driver_nio.sh"]
