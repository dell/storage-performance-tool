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
ARG VERSION
COPY --from=0 /build/libs/spt-storage-driver-http-${VERSION}.jar /tmp/
RUN mv -f /tmp/spt-storage-driver-http-*.jar /opt/spt/ext/ \
    && curl http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-coop/${STORAGE_DRIVER_COOP_VERSION}/spt-storage-driver-coop-${STORAGE_DRIVER_COOP_VERSION}.jar -o /opt/spt/ext/spt-storage-driver-coop.jar \
    && curl http://repo.maven.apache.org/maven2/io/github/dell/spt/spt-storage-driver-netty/${STORAGE_DRIVER_NETTY_VERSION}/spt-storage-driver-netty-${STORAGE_DRIVER_NETTY_VERSION}.jar -o /opt/spt/ext/spt-storage-driver-netty.jar \
