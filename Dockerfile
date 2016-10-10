FROM php:7
MAINTAINER Martin Halamicek <martin@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
  && apt-get install unzip git unixODBC-dev libpq-dev -y

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini


RUN docker-php-ext-install pdo_pgsql

# https://github.com/docker-library/php/issues/103
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete


## install snowflake drivers
ADD ./snowflake_linux_x8664_odbc.tgz /usr/bin
ADD ./docker/snowflake/simba.snowflake.ini /etc/simba.snowflake.ini
ADD ./docker/snowflake/odbcinst.ini /etc/odbcinst.ini
RUN mkdir -p  /usr/bin/snowflake_odbc/log

ENV SIMBAINI /etc/simba.snowflake.ini
ENV SSL_DIR /usr/bin/snowflake_odbc/SSLCertificates/nssdb
ENV LD_LIBRARY_PATH /usr/bin/snowflake_odbc/lib


RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

ADD ./ /code

WORKDIR /code
RUN composer install --prefer-dist --no-interaction





