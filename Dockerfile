ARG PHP_VERSION=8.2
# the default env bellow is used when build pipeline sends "PHP_VERSION=" - the above default value is ignored in that case
FROM php:${PHP_VERSION:-8.2}-cli-bullseye AS dev
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
    && apt-get update -q \
    && ACCEPT_EULA=Y apt-get install \
        unzip \
        git \
        libpq-dev \
        -y --no-install-recommends \
    && rm -r /var/lib/apt/lists/* \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini


# snowflake - charset settings
ENV LANG en_US.UTF-8

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
