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

## Download timeouts and retries

File downloads (`Client::downloadFile()`, `Client::downloadSlicedFile()` and `TableExporter`) are
not bounded by object size on AWS — a download of any realistic size can finish, as long as it
keeps making progress. Behaviour differs per file storage provider:

**The three providers do not behave the same.** Only the AWS path has been given a deliberate
transfer policy; Azure and GCP still run on their SDK defaults, and each has a different effective
ceiling on how large a file it can download:

| | AWS (`S3ClientFactory`) | Azure (`BlobClientFactory`) | GCP (`GcsClientFactory`) |
| --- | --- | --- | --- |
| Total request deadline | 12 h liveness backstop | **120 s per blob request** | none (`requestTimeout` null) |
| Stall detection | below 1 KB/s for 60 s | none | none |
| Connect timeout | 10 s | 10 s | not set |
| Retries | `awsRetries`, default `Client::DEFAULT_RETRIES_COUNT` (15) | 5, exponential (`BlobStorageRetryMiddleware`) | 3 (Google client default) |
| Writes to disk by streaming | yes (`SaveAs`) | yes | **no in `GcsDownloader`** — see below |
| Effective size ceiling | ~3.3 TB at 80 MB/s | **~10 GB at 80 MB/s** | PHP `memory_limit` |

Notes:

- The AWS deadline is a liveness backstop, not a size cap. Stall detection alone cannot guarantee
  termination: it only fires *below* 1 KB/s and needs the whole 60 s window under the limit, so a
  link crawling just above that would otherwise run for months (40 GB at 1 KB/s is over a year).
  The deadline is sized so no healthy transfer of any plausible export can reach it.
- Retries restart the whole object transfer from the first byte on every provider, so each retry
  pays full egress. Keep `awsRetries` low if you download very large files.
- Guzzle's `read_timeout` option is honoured only by its `StreamHandler`. The AWS SDK uses the cURL
  handler, where the equivalent is `CURLOPT_LOW_SPEED_LIMIT` / `CURLOPT_LOW_SPEED_TIME`.
- The Azure 120 s deadline caps a single blob download and is currently the strictest limit of the
  three. Known limitation, tracked separately.
- `GcsDownloader` downloads via `StorageObject::downloadAsString()`, which materialises the whole
  object in PHP memory before writing it out, so on GCP the binding limit is `memory_limit` rather
  than any timeout. `Client::downloadGcsFile()` does not share this problem — it streams via
  `downloadToFile()`. Tracked separately.

## License

See [LICENSE](./LICENSE) file.
