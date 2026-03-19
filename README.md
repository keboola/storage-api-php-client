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

## License

See [LICENSE](./LICENSE) file.
