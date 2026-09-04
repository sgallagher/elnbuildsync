FROM quay.io/fedora/fedora:44

WORKDIR /tmp

COPY . .

RUN INSTALL_PKGS=" \
    bash \
    bodhi-client \
    fedora-messaging \
    koji \
    krb5-workstation \
    python3-cachetools \
    python3-click \
    python3-fastapi \
    python3-GitPython \
    python3-gssapi \
    python3-httpx \
    python3-krb5 \
    python3-pip \
    python3-pyyaml \
    python3-requests \
    python3-rpm \
    python3-sqlalchemy+postgresql_asyncpg \
    python3-tenacity \
    python3-twisted \
    python3-uvicorn \
    python3-virtualenv \
    " && \
    dnf -y --setopt=tsflags=nodocs install $INSTALL_PKGS && \
    dnf -y clean all --enablerepo='*'

# Copy in the Koji config files rather than installing fedora-packager because
# it pulls in far too many dependencies.
COPY koji_config/fedora.conf /etc/koji.conf.d/fedora.conf
COPY koji_config/stg.conf /etc/koji.conf.d/stg.conf

RUN mkdir -p /etc/elnbuildsync && \
    chgrp -R 0 /etc/elnbuildsync && \
    chmod -R g=u /etc/elnbuildsync

USER 1001
EXPOSE 8080

ENTRYPOINT ["/tmp/run.sh"]
