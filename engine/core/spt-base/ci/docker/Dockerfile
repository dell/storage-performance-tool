FROM eclipse-temurin:21-jdk-alpine

ADD src ./src/
ADD gradle ./gradle/
ADD build.gradle ./build.gradle
ADD gradlew ./gradlew
ADD settings.gradle ./settings.gradle
RUN ./gradlew clean jre

FROM alpine:3.8.4

RUN apk --update add --no-cache ca-certificates curl openssl binutils xz \
    && GLIBC_VER="2.28-r0" \
    && ALPINE_GLIBC_REPO="https://github.com/sgerrand/alpine-pkg-glibc/releases/download" \
    && GCC_LIBS_URL="https://archive.archlinux.org/packages/g/gcc-libs/gcc-libs-8.3.0-1-x86_64.pkg.tar.xz" \
    && GCC_LIBS_SHA256=400e2ecb1b2dfb40e09cdb6805f0075cbc88e6fcef9b73f23c64a6e709dcd61b \
    && ZLIB_URL="https://archive.archlinux.org/packages/z/zlib/zlib-1%3A1.2.11-3-x86_64.pkg.tar.xz" \
    && ZLIB_SHA256=17aede0b9f8baa789c5aa3f358fbf8c68a5f1228c5e6cba1a5dd34102ef4d4e5 \
    && curl -Lks https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub -o /etc/apk/keys/sgerrand.rsa.pub \
    && curl -Lks ${ALPINE_GLIBC_REPO}/${GLIBC_VER}/glibc-${GLIBC_VER}.apk > /tmp/${GLIBC_VER}.apk \
    && apk add --allow-untrusted /tmp/${GLIBC_VER}.apk \
    && curl -Lks ${GCC_LIBS_URL} -o /tmp/gcc-libs.tar.xz \
    && echo "${GCC_LIBS_SHA256}  /tmp/gcc-libs.tar.xz" | sha256sum -c - \
    && mkdir /tmp/gcc \
    && tar -xf /tmp/gcc-libs.tar.xz -C /tmp/gcc \
    && mv /tmp/gcc/usr/lib/libgcc* /tmp/gcc/usr/lib/libstdc++* /usr/glibc-compat/lib \
    && strip /usr/glibc-compat/lib/libgcc_s.so.* /usr/glibc-compat/lib/libstdc++.so* \
    && curl -Lks ${ZLIB_URL} -o /tmp/libz.tar.xz \
    && echo "${ZLIB_SHA256}  /tmp/libz.tar.xz" | sha256sum -c - \
    && mkdir /tmp/libz \
    && tar -xf /tmp/libz.tar.xz -C /tmp/libz \
    && mv /tmp/libz/usr/lib/libz.so* /usr/glibc-compat/lib \
    && apk del binutils \
    && rm -rf /tmp/${GLIBC_VER}.apk /tmp/gcc /tmp/gcc-libs.tar.xz /tmp/libz /tmp/libz.tar.xz /var/cache/apk/*

COPY --from=0 /build/jre /opt/spt
COPY --from=0 /build/libs/spt-*.jar /opt/spt/
ADD ci/docker/entrypoint.sh /opt/spt/entrypoint.sh


RUN ln -s /opt/spt/spt-*.jar /opt/spt/spt.jar; \
    mkdir ${HOME}/.spt; \
    chmod -R ugo+rwx ${HOME}/.spt; \
    chmod -R +x /opt/spt/bin; \
    chmod +x /opt/spt/entrypoint.sh;

ENTRYPOINT ["/opt/spt/entrypoint.sh"]
