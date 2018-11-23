FROM quay.io/keboola/aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
# How to update drivers - https://github.com/keboola/drivers-management
RUN /usr/bin/aws s3 cp s3://keboola-drivers/snowflake/snowflake-odbc-2.16.10.x86_64.deb /tmp/snowflake-odbc.deb

FROM php:7.1
MAINTAINER Martin Halamicek <martin@keboola.com>
ENV DEBIAN_FRONTEND noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
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
COPY --from=0 /tmp/snowflake-odbc.deb /tmp/snowflake-odbc.deb
RUN dpkg -i /tmp/snowflake-odbc.deb
ADD ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

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
