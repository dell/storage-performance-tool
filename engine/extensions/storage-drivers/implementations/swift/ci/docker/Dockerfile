ARG BASE_VERSION

FROM eclipse-temurin:21-alpine
ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jar

FROM ghcr.io/dell/storage-performance-tool:${BASE_VERSION}
ARG BASE_VERSION
ARG STORAGE_DRIVER_COOP_VERSION
ARG STORAGE_DRIVER_NETTY_VERSION
ARG STORAGE_DRIVER_HTTP_VERSION
ARG VERSION
RUN mkdir -p $HOME/.spt/${BASE_VERSION}/ext
COPY --from=0 /build/libs/spt-storage-driver-swift-${VERSION}.jar /tmp/
ADD ci/docker/entrypoint_storage_driver_swift.sh /opt/spt/entrypoint_storage_driver_swift.sh
RUN mv -f /tmp/spt-storage-driver-swift-*.jar $HOME/.spt/${BASE_VERSION}/ext/ \
    && chmod +x /opt/spt/entrypoint_storage_driver_swift.sh \
    && curl http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/spt-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o $HOME/.spt/${BASE_VERSION}/ext/spt-storage-driver-coop.jar \
    && curl http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-netty/${STORAGE_DRIVER_NETTY_VERSION}/spt-storage-driver-netty-${STORAGE_DRIVER_NETTY_VERSION}.jar -o $HOME/.spt/${BASE_VERSION}/ext/spt-storage-driver-netty.jar \
    && curl http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-http/${STORAGE_DRIVER_HTTP_VERSION}/spt-storage-driver-http-${STORAGE_DRIVER_HTTP_VERSION}.jar -o $HOME/.spt/${BASE_VERSION}/ext/spt-storage-driver-http.jar
ENTRYPOINT ["/opt/spt/entrypoint_storage_driver_swift.sh"]
