ARG BASE_VERSION

FROM eclipse-temurin:21-alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM dcr.octo.dell.com/spt/spt-base:${BASE_VERSION}
ARG BASE_VERSION
ARG VERSION
RUN mkdir -p $HOME/.spt/${BASE_VERSION}/ext
COPY --from=0 /build/libs/spt-storage-driver-coop-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_coop.sh /opt/spt/entrypoint_storage_driver_coop.sh
RUN mv -f /tmp/spt-storage-driver-coop-*.jar $HOME/.spt/${BASE_VERSION}/ext/; \
    chmod +x /opt/spt/entrypoint_storage_driver_coop.sh
ENTRYPOINT ["/opt/spt/entrypoint_storage_driver_coop.sh"]
