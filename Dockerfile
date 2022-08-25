FROM quay.io/keboola/aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
RUN /usr/bin/aws s3 cp s3://keboola-drivers/teradata/tdodbc1710-17.10.00.08-1.x86_64.deb /tmp/teradata/tdodbc.deb
RUN /usr/bin/aws s3 cp s3://keboola-drivers/teradata/utils/TeradataToolsAndUtilitiesBase__ubuntu_x8664.17.00.34.00.tar.gz  /tmp/teradata/tdutils.tar.gz

ARG PHP_VERSION=7.4
# the default env bellow is used when build pipeline sends "PHP_VERSION=" - the above default value is ignored in that case
FROM php:${PHP_VERSION:-7.4}-cli-buster as dev
MAINTAINER Martin Halamicek <martin@keboola.com>
ENV DEBIAN_FRONTEND noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG SNOWFLAKE_ODBC_VERSION=2.21.0
ARG SNOWFLAKE_GPG_KEY=EC218558EABB25A1
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ARG SYNAPSE_ODBC_VERSION=5.9.0

WORKDIR /code/

COPY docker/composer-install.sh /tmp/composer-install.sh

# Locale
ENV LC_CTYPE=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get update -q \
    && apt-get install gnupg -y --no-install-recommends \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update -q \
    && ACCEPT_EULA=Y apt-get install \
        unzip \
        git \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        debsig-verify \
        dirmngr \
        libonig-dev \
        libxml2-dev \
        -y --no-install-recommends \
    && rm -r /var/lib/apt/lists/* \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini


RUN docker-php-ext-install pdo_pgsql

# https://github.com/docker-library/php/issues/103#issuecomment-353674490
RUN set -ex; \
    docker-php-source extract; \
    { \
        echo '# https://github.com/docker-library/php/issues/103#issuecomment-353674490'; \
        echo 'AC_DEFUN([PHP_ALWAYS_SHARED],[])dnl'; \
        echo; \
        cat /usr/src/php/ext/odbc/config.m4; \
    } > temp.m4; \
    mv temp.m4 /usr/src/php/ext/odbc/config.m4; \
    docker-php-ext-configure odbc --with-unixODBC=shared,/usr; \
    docker-php-ext-install odbc; \
    docker-php-source delete


## install snowflake drivers
COPY ./docker/snowflake/generic.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY/generic.pol
ADD https://sfc-repo.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb /tmp/snowflake-odbc.deb
COPY ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

# snowflake - charset settings
ENV LANG en_US.UTF-8

RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY \
    && if ! gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_GPG_KEY; then \
        gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_GPG_KEY;  \
    fi \
    && gpg --export $SNOWFLAKE_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY/debsig.gpg \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb

# Synapse ODBC
RUN set -ex; \
    pecl install sqlsrv-$SYNAPSE_ODBC_VERSION pdo_sqlsrv-$SYNAPSE_ODBC_VERSION; \
    docker-php-ext-enable sqlsrv pdo_sqlsrv; \
    docker-php-source delete

# Exasol
RUN set -ex; \
    mkdir -p /tmp/exasol/odbc /opt/exasol ;\
    curl https://www.exasol.com/support/secure/attachment/161242/EXASOL_ODBC-7.1.1.tar.gz --output /tmp/exasol/odbc.tar.gz; \
    tar -xzf /tmp/exasol/odbc.tar.gz -C /tmp/exasol/odbc --strip-components 1; \
    cp /tmp/exasol/odbc/lib/linux/x86_64/libexaodbc-uo2214lv2.so /opt/exasol/;\
    echo "\n[exasol]\nDriver=/opt/exasol/libexaodbc-uo2214lv2.so\n" >> /etc/odbcinst.ini;\
    rm -rf /tmp/exasol;

# Teradata
# Teradata ODBC
COPY --from=0 /tmp/teradata/tdodbc.deb /tmp/teradata/tdodbc.deb
COPY docker/teradata/odbc.ini /tmp/teradata/odbc_td.ini
COPY docker/teradata/odbcinst.ini /tmp/teradata/odbcinst_td.ini

RUN dpkg -i /tmp/teradata/tdodbc.deb \
    && cat /tmp/teradata/odbc_td.ini >> /etc/odbc.ini \
    && cat /tmp/teradata/odbcinst_td.ini >> /etc/odbcinst.ini \
    && rm -rf /tmp/teradata

ENV ODBCHOME = /opt/teradata/client/ODBC_64/
ENV ODBCINI = /opt/teradata/client/ODBC_64/odbc.ini
ENV ODBCINST = /opt/teradata/client/ODBC_64/odbcinst.ini
ENV LD_LIBRARY_PATH = /opt/teradata/client/ODBC_64/lib

# Teradata Utils
COPY --from=0 /tmp/teradata/tdutils.tar.gz /tmp/teradata/tdutils.tar.gz
RUN cd /tmp/teradata \
    && tar -xvaf tdutils.tar.gz \
    && sh /tmp/teradata/TeradataToolsAndUtilitiesBase/.setup.sh tptbase s3axsmod \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/teradata

# PHP modules
# needed for Exasol and Teradata
RUN docker-php-ext-configure pdo_odbc --with-pdo-odbc=unixODBC,/usr \
  && docker-php-ext-install pdo_odbc

WORKDIR /code

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* ./
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . .
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS
