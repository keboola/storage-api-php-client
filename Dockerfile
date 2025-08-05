ARG PHP_VERSION=8.4
# the default env bellow is used when build pipeline sends "PHP_VERSION=" - the above default value is ignored in that case
FROM php:${PHP_VERSION:-8.4}-cli-bullseye AS dev
MAINTAINER Martin Halamicek <martin@keboola.com>
ENV DEBIAN_FRONTEND noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG SNOWFLAKE_ODBC_VERSION=3.4.1
ARG SNOWFLAKE_GPG_KEY=630D9F3CAB551AF3
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ARG SYNAPSE_ODBC_VERSION=5.12.0

WORKDIR /code/

COPY docker/composer-install.sh /tmp/composer-install.sh

# Locale
ENV LC_CTYPE=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get update -q \
    && apt-get install gnupg -y --no-install-recommends \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update -q \
    && ACCEPT_EULA=Y apt-get install \
        unzip \
        git \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        debsig-verify \
        dirmngr \
        msodbcsql17 \
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

# PHP modules
RUN docker-php-ext-install bcmath

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
