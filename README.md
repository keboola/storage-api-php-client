# Keboola Storage API PHP client
[![Latest Stable Version](https://poser.pugx.org/keboola/storage-api-client/v/stable.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![License](https://poser.pugx.org/keboola/storage-api-client/license.svg)](https://packagist.org/packages/keboola/storage-api-client)
[![Total Downloads](https://poser.pugx.org/keboola/storage-api-client/downloads.svg)](https://packagist.org/packages/keboola/storage-api-client)

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

**Never run this tests on production project with real data, always create project for testing purposes!!!**

When the parameters are set you can run tests by **php vendor/bin/phpunit** command.

## Release History
See the [CHANGELOG](CHANGELOG.md).

