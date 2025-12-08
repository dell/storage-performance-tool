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
ARG VERSION
COPY --from=0 /build/libs/spt-load-step-pipeline-${VERSION}.jar /tmp/
RUN mkdir -p $HOME/.spt/${BASE_VERSION}/ext
RUN mv -f /tmp/spt-load-step-pipeline-*.jar $HOME/.spt/${BASE_VERSION}/ext/
