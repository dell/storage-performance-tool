ARG BASE_VERSION

FROM eclipse-temurin:21-alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar \
    && ls -l build/libs

FROM dellspt/spt-base:${BASE_VERSION}
ARG BASE_VERSION
ARG STORAGE_DRIVER_COOP_VERSION
ARG VERSION
RUN mkdir -p $HOME/.spt/${BASE_VERSION}/ext
COPY --from=0 /build/libs/spt-storage-driver-nio-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_nio.sh /opt/spt/entrypoint_storage_driver_nio.sh
RUN mv -f /tmp/spt-storage-driver-nio-*.jar $HOME/.spt/${BASE_VERSION}/ext/ \
    && chmod +x /opt/spt/entrypoint_storage_driver_nio.sh \
    && curl http://repo.maven.apache.org/maven2/com/github/dell-spt/spt-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/spt-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o $HOME/.spt/${BASE_VERSION}/ext/spt-storage-driver-coop.jar
ENTRYPOINT ["/opt/spt/entrypoint_storage_driver_nio.sh"]
