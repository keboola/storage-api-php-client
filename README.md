# Keboola Storage API PHP client
[![Latest Stable Version](https://poser.pugx.org/keboola/storage-api-client/v/stable.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![License](https://poser.pugx.org/keboola/storage-api-client/license.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Total Downloads](https://poser.pugx.org/keboola/storage-api-client/downloads.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Build Status](https://travis-ci.org/keboola/storage-api-php-client.svg?branch=master)](https://travis-ci.org/keboola/storage-api-php-client)

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
        "php" : ">=5.4.0",
        "keboola/storage-api-client": "2.12.*"
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
]);
$csvFile = new CsvFile(__DIR__ . '/my.csv', ',', '"');
$client->writeTableAsync('in.c-main.my-table', $csvFile);
```

Table export to file:

```php
require 'vendor/autoload.php';

use Keboola\StorageApi\Client,
  Keboola\StorageApi\TableExporter;

$client = new Client(['token' => 'YOUR_TOKEN',]);

$exporter = new TableExporter($client);
$exporter->exportTable('in.c-main.my-table', './in.c-main.my-table.csv', []);

```

## Tests
Tests requires valid Storage API token and URL of API.
You can set these by copying file config.template.php into config.php and filling required constants int config.php file. Other way to provide parameters is to set environment variables:

    export=STORAGE_API_URL=http://connection.keboola.com
    export=STORAGE_API_TOKEN=YOUR_TOKEN

Tests expects master token and performs all operations including bucket and table deletes on project storage associated to token.
 
### Redshift tests

Reshift tests require a cluster connected to Storage API and credentials. When you have a project with enabled Redshift, create 2 Redshift buckets:
 
   - in.c-api-tests-redshift
   - out.c-api-tests-redshift

Then you can create your Redshift user:

 - Connect to your Redshift database `sapi_YOURPROJECTID` and run queries:

	    CREATE USER test_user PASSWORD '***';
	    GRANT ALL PRIVILEGES ON DATABASE sapi_YOURPROJECTID TO test_user;
	    GRANT ALL PRIVILEGES ON SCHEMA "in.c-api-tests-redshift" TO test_user;
	    GRANT ALL PRIVILEGES ON SCHEMA "out.c-api-tests-redshift" TO test_user;
	    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "in.c-api-tests-redshift" TO test_user;
	    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "out.c-api-tests-redshift" TO test_user;
    
And then assign Redshift related env variables

    export REDSHIFT_HOSTNAME=sapi-06-default.cmizbsfmzc6w.us-east-1.redshift.amazonaws.com
    export REDSHIFT_USER=test_user 
    export REDSHIFT_PASSWORD=***

**Never run this tests on production project with real data, always create project for testing purposes!!!**

When the parameters are set you can run tests by **php vendor/bin/phpunit** command.

## Versioning
[semver.org](http://semver.org/) is followed.

## Release History
See the [CHANGELOG](CHANGELOG.md).

