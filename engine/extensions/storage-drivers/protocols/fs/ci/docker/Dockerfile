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
ARG STORAGE_DRIVER_COOP_VERSION
ARG STORAGE_DRIVER_NIO_VERSION
ARG VERSION
RUN mkdir -p $HOME/.spt/${BASE_VERSION}/ext
COPY --from=0 /build/libs/spt-storage-driver-fs-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_fs.sh /opt/spt/entrypoint_storage_driver_fs.sh
RUN mv -f /tmp/spt-storage-driver-fs-*.jar $HOME/.spt/${BASE_VERSION}/ext/ \
    && chmod +x /opt/spt/entrypoint_storage_driver_fs.sh \
    && curl https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/spt-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o $HOME/.spt/${BASE_VERSION}/ext/spt-storage-driver-coop.jar \
    && curl https://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-nio/${STORAGE_DRIVER_NIO_VERSION}/spt-storage-driver-nio-${STORAGE_DRIVER_NIO_VERSION}.jar -o $HOME/.spt/${BASE_VERSION}/ext/spt-storage-driver-nio.jar
ENTRYPOINT ["/opt/spt/entrypoint_storage_driver_fs.sh"]
