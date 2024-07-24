# Keboola Storage API PHP Client

[![Latest Stable Version](https://poser.pugx.org/keboola/storage-api-client/v/stable.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![License](https://poser.pugx.org/keboola/storage-api-client/license.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Total Downloads](https://poser.pugx.org/keboola/storage-api-client/downloads.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Build on tag](https://github.com/keboola/storage-api-php-client/actions/workflows/tag.yml/badge.svg)](https://github.com/keboola/storage-api-php-client/actions/workflows/tag.yml)

Simple PHP wrapper library for [Keboola Storage API](http://docs.keboola.apiary.io/).

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
        "php" : ">=8.1",
        "keboola/storage-api-client": "^14.0"
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

Read more in [Composer documentation](http://getcomposer.org/doc/01-basic-usage.md).

## Usage examples

Table write:

```php
require 'vendor/autoload.php';

use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

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

use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'url' => 'https://connection.keboola.com'
]);

$exporter = new TableExporter($client);
$exporter->exportTable('in.c-main.my-table', './in.c-main.my-table.csv', []);

```

## Tests

**Warning: Never run this tests on production project with real data, always create project for testing purposes!!!**

*Note: For automated tests, the tests are run again three times by default if they fail. For local development this would be quite annoying,
so you can disable this by creating new file `phpunit-retry.xml` from `phpunit-retry.xml.dist`*

The main purpose of these tests is "black box" test driven development of Keboola Connection. These tests guards the API implementation.

Tests should be executed against local dockerized version of [Keboola Connection](https://github.com/keboola/connection/) (private repo).
These tests and local KBC are configured to share docker network where the Storage API and Manage API endpoints are provided. 
These APIs are available at `http://connection-apache/` endpoint from clients tests.


Before executing tests please install dev dependencies:
- Teradata driver download keys (AWS keys with access to `keboola-drivers` bucket):
  - `export DRIVER_DOWNLOADS_ACCESS_KEY_ID=...`
  - `export DRIVER_DOWNLOADS_SECRET_ACCESS_KEY=...`
- `docker compose build`
- `docker compose run --rm dev composer install`

Tests are divided into multiple test suites.

### Common test suite
This test suite expects following environment variables set:
 - `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
 - `STORAGE_API_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. There are no special requirements for project storage backend.
 - `STORAGE_API_GUEST_TOKEN` - Storage API token associated to user (Admin master token) with guest role in same project as `STORAGE_API_TOKEN`.
 - `STORAGE_API_READ_ONLY_TOKEN` - Storage API token associated to user (Admin master token) with readOnly role in same project as `STORAGE_API_TOKEN`.
 - `STORAGE_API_SHARE_TOKEN` - Storage API token associated to user (Admin master token) with share role in same project as `STORAGE_API_TOKEN`.
 - `STORAGE_API_MAINTENANCE_URL` - URL for maintenance testing (https://maintenance-testing.keboola.com/)


You can export variables manually, or you can create and fill file `set-env.sh` as copy of attached `set-env.template.sh`.

Then you can run tests:

`source ./set-env.sh &&  docker compose run --rm dev vendor/bin/phpunit --testsuite common`

 
### Redshift backend test suite

This test suite expects following environment variables set:
- `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
- `STORAGE_API_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. **Project must have `Redshift` set as default backend.**
- `REDSHIFT_NODE_COUNT` - (optional) Set Redshift node count `default=1`

You can export variables manually, or you can create and fill file `set-env.redshift.sh`
as copy of attached `set-env.redshift.template.sh`.

Then you can run tests:

`source ./set-env.redshift.sh && docker compose run --rm dev vendor/bin/phpunit --testsuite backend-redshift-part-1`
`source ./set-env.redshift.sh && docker compose run --rm dev vendor/bin/phpunit --testsuite backend-redshift-part-2`

### Snowflake backend test suite
This test suite expects following environment variables set:
- `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
- `STORAGE_API_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. **Project must have `snowflake` set as default backend.**


You can run these tests in docker:

`source ./set-env.snowflake.sh && docker compose run --rm dev vendor/bin/phpunit --testsuite backend-snowflake-part-1`
`source ./set-env.snowflake.sh && docker compose run --rm dev vendor/bin/phpunit --testsuite backend-snowflake-part-2`

### Mixed backend test suite
Project can support multiple backends, this is useful for migrations from one backend to another.
These tests require project with all backend assigned (redshift, snowflake).

This test suite expects following environment variables set:
 - `STORAGE_API_URL` - URL of Keboola Storage API (https://connection.keboola.com/)
 - `STORAGE_API_TOKEN` and `STORAGE_API_LINKING_TOKEN` - Storage API token associated to user (Admin master token) with all permissions. Project must have assigned `snowflake` and `redshift` backend. STORAGE_API_TOKEN and STORAGE_API_LINKING_TOKEN have to be tokens to different project in same organization.
 - `STORAGE_API_GUEST_TOKEN` - Storage API token associated to user (Admin master token) with guest role in same project as `STORAGE_API_TOKEN`.
 - `STORAGE_API_SHARE_TOKEN` - Storage API token associated to user (Admin master token) with share role in same project as `STORAGE_API_TOKEN`.
 - `STORAGE_API_MAINTENANCE_URL` - URL for maintenance testing (https://maintenance-testing.keboola.com/)
 - `STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION` - Storage API token associated to project in the same organization as `STORAGE_API_TOKEN` but with different admin as `STORAGE_API_TOKEN`.
 - `STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION` - Storage API token associated to other admin as `STORAGE_API_TOKEN` and project in the other organization as `STORAGE_API_TOKEN`.
You can export variables manually or you can create and fill file `set-env.mixed.sh` as copy of attached `set-env.mixed.template.sh`.

Then you can run tests:

`source ./set-env.mixed.sh && docker compose run --rm dev vendor/bin/phpunit --testsuite backend-mixed'`

## Running test from PHPStorm

The whole test suite is quite big and it can take few hours. So it is a good idea to run just a testcase which you are interested in from PHPStorm, or you can run them from a console (using `--filter` option). 

**How to set up PHPStorm for running tests:**
- go to Settings / Languages & Frameworks / PHP
- row CLI -> three dots
- Plus button -> `From Docker, Vagrant, VM, WSL, Remote...`
- Select `Docker Compose`; Service `dev-xdebug`; Environment variables define value from your `set-env.php`
    - an easy way how to do it is copy content of `set-env.php` without `export ` prefix -> click on the icon in  Env. vars. -> click on the paste icon. It should pass all the key=value entries in the window. If it doesn't work, set them manually.
- Set Path mappings to `<Project root> -> /code` (`<Project root>` is an absolute path to the project directory) 
- hint: create different interpreters for different environments

_Note: see [this link](https://www.jetbrains.com/help/phpstorm/configuring-remote-interpreters.html) for more information and screenshots about the description above._

## License

See [LICENSE](./LICENSE) file.
