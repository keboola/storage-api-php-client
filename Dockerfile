FROM php:7.1
MAINTAINER Martin Halamicek <martin@keboola.com>
ENV DEBIAN_FRONTEND noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG SNOWFLAKE_ODBC_VERSION=2.19.15
ARG SNOWFLAKE_GPG_KEY=EC218558EABB25A1
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update && apt-get install -y --no-install-recommends \
        unzip \
        git \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        gpg \
        debsig-verify \
        dirmngr \
        gpg-agent \
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
    && gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_GPG_KEY \
    && gpg --export $SNOWFLAKE_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY/debsig.gpg \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb

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
