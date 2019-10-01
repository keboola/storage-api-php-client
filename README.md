# Keboola Storage API PHP client
[![Latest Stable Version](https://poser.pugx.org/keboola/storage-api-client/v/stable.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![License](https://poser.pugx.org/keboola/storage-api-client/license.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Total Downloads](https://poser.pugx.org/keboola/storage-api-client/downloads.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Build Status](https://travis-ci.com/keboola/storage-api-php-client.svg?branch=master)](https://travis-ci.com/keboola/storage-api-php-client)

Simple PHP wrapper library for [Keboola Storage REST API](http://docs.keboola.apiary.io/)

## Installation

Library is available as composer package.
To start using composer in your project follow these steps:

**Install composer**
  
```bash
curl -s http://getcomposer.org/installer | php
mv ./composer.phar ~/bin/composer # or /usr/local/bin/composer
```

**Create composer.json file in your project root folder:**
```json
{
    "require": {
        "php" : ">=5.6.0",
        "keboola/storage-api-client": "^9.0"
    }
}
```

**Install package:**

```bash
composer install
```

**Add autoloader in your bootstrap script:**

```php
require 'vendor/autoload.php';
```

Read more in [Composer documentation](http://getcomposer.org/doc/01-basic-usage.md)

## Usage examples

Table write:

```php
require 'vendor/autoload.php';

use Keboola\StorageApi\Client,
	Keboola\Csv\CsvFile;

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'url' => 'https://connection.keboola.com'
]);
$csvFile = new CsvFile(__DIR__ . '/my.csv', ',', '"');
$client->writeTableAsync('in.c-main.my-table', $csvFile);
```

Table export to file:

```php
require 'vendor/autoload.php';

use Keboola\StorageApi\Client,
  Keboola\StorageApi\TableExporter;

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'url' => 'https://connection.keboola.com'
]);

$exporter = new TableExporter($client);
$exporter->exportTable('in.c-main.my-table', './in.c-main.my-table.csv', []);

```

## Tests

**Warning: Never run this tests on production project with real data, always create project for testing purposes!!!**

The main purpose of these test is "black box" test driven development of Keboola Connection. These test guards the API implementation.

Tests should be executed against local dockerized version of [Keboola Connection](https://github.com/keboola/connection/) (private repo).
These tests and local KBC are configured to share docker network where the Storage and Manage API endpoints are provided. 
These APIs are available at `http://connection-apache/` endpoint from clients tests.


Before executing tests please install dev dependencies:
- `docker-compose build`
- `docker-compose run --rm dev composer install`

Tests are divided into multiple test suites.

### Common test suite
This test suite expects following environment variables set:
 - `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
 - `STORAGE_API_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. There are no special requirements for project storage backend.
 - `STORAGE_API_MAINTENANCE_URL` - URL for maintenance testing (https://maintenance-testing.keboola.com/)


You can export variables manually or you can create and fill file `set-env.sh` as copy of attached `set-env.template.sh`.

Than  you can run tests:

`source ./set-env.sh &&  docker-compose run --rm dev vendor/bin/phpunit --testsuite common`

 
### Redshift backend test suite

This test suite expects following environment variables set:
- `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
- `STORAGE_API_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. **Project must have `Redshift` set as default backend.**

You can export variables manually or you can create and fill file `set-env.redshift.sh`
as copy of attached `set-env.redshift.template.sh`.

Than  you can run tests:

`source ./set-env.redshift.sh && docker-compose run --rm dev vendor/bin/phpunit --testsuite backend-redshift-part-1`
`source ./set-env.redshift.sh && docker-compose run --rm dev vendor/bin/phpunit --testsuite backend-redshift-part-2`

### Snowflake backend test suite
This test suite expects following environment variables set:
- `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
- `STORAGE_API_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. **Project must have `snowflake` set as default backend.**


You can run these tests in docker:

`source ./set-env.snowflake.sh && docker-compose run --rm dev vendor/bin/phpunit --testsuite backend-snowflake-part-1`
`source ./set-env.snowflake.sh && docker-compose run --rm dev vendor/bin/phpunit --testsuite backend-snowflake-part-2`

### Mixed backend test suite
Project can support multiple backends, this is useful for migrations from one backend to another.
These tests require project with all backend assigned (redshift, snowflake).

This test suite expects following environment variables set:
 - `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
 - `STORAGE_API_TOKEN` and `STORAGE_API_LINKING_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. Project must have assigned `snowflake` and `redshift` backend. STORAGE_API_TOKEN and STORAGE_API_LINKING_TOKEN have to be tokens to different project in same organization.
 - `STORAGE_API_MAINTENANCE_URL` - URL for maintenance testing (https://maintenance-testing.keboola.com/)
 - `STORAGE_API_PROJECT_IDS_IN_ORGANIZATION` - IDs of projects in organization, where one must be ID associated with `STORAGE_API_TOKEN`
 - `STORAGE_API_PROJECT_ID_AVAILABLE_TO_LINK` - ID of project in organization, which is associated with `STORAGE_API_LINKING_TOKEN` but it must be different from the `STORAGE_API_PROJECT_IDS_IN_ORGANIZATION`
 - `STORAGE_API_PROJECT_ID_NOT_AVAILABLE_TO_LINK` - ID of project in organization, which is not associated with `STORAGE_API_LINKING_TOKEN`
 - `STORAGE_API_PROJECT_IDS_NOT_IN_ORGANIZATION` - IDs of projects in other organization

You can export variables manually or you can create and fill file `set-env.mixed.sh` as copy of attached `set-env.mixed.template.sh`.

Than  you can run tests:

`source ./set-env.mixed.sh && docker-compose run --rm dev vendor/bin/phpunit --testsuite backend-mixed'`



## Versioning
[semver.org](http://semver.org/) is followed.

## Release History
See the [CHANGELOG](CHANGELOG.md).

