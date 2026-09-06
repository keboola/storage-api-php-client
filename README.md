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
not bounded by object size on any provider — a download of any realistic size can finish, as long
as it keeps making progress. The three providers share one deliberate transfer policy:

| | AWS (`S3ClientFactory`) | Azure (`BlobClientFactory`) | GCP (`GcsClientFactory`) |
| --- | --- | --- | --- |
| Total request deadline | 12 h liveness backstop | 12 h liveness backstop | 12 h liveness backstop |
| Stall detection | below 1 KB/s for 60 s | below 1 KB/s for 60 s | below 1 KB/s for 60 s |
| Connect timeout | 10 s | 10 s | 10 s |
| Retries | `awsRetries`, default `Client::DEFAULT_RETRIES_COUNT` (15) | 5, exponential (`BlobStorageRetryMiddleware`) | 3 (Google client default) |
| Writes to disk | directly (`SaveAs` becomes Guzzle's `sink`) | via `php://temp`, then copied | via `php://temp`, then copied (`downloadToFile()`) |
| Effective size ceiling | ~3.3 TB at 80 MB/s | ~3.3 TB at 80 MB/s | ~3.3 TB at 80 MB/s |

Notes:

- The deadlines are liveness backstops, not size caps. Stall detection alone cannot guarantee
  termination: it only fires *below* 1 KB/s and needs the whole 60 s window under the limit, so a
  link crawling just above that would otherwise run for months (40 GB at 1 KB/s is over a year).
  They are sized so no healthy transfer of any plausible export can reach them.
- Retries restart the whole object transfer from the first byte on AWS and Azure, so each retry pays
  full egress. Keep `awsRetries` low if you download very large files. On GCP an interruption that
  still carried a 2xx response resumes from the last fetched byte with a `Range` header
  (`Rest::downloadObject()`); any other failure restarts from the first byte.
- Guzzle's `read_timeout` option is honoured only by its `StreamHandler`. All three download clients
  use the cURL handler, where the equivalent is `CURLOPT_LOW_SPEED_LIMIT` / `CURLOPT_LOW_SPEED_TIME`.
- Azure downloads force the cURL handler (`BlobClientFactory::createDownloadClient()`). The Azure
  SDK requests blob bodies with Guzzle's `stream` option, which the default handler routes to the
  `StreamHandler`: there `curl` options and `connect_timeout` are ignored, `timeout` is a per-read
  socket timeout rather than a total deadline, and a stalled transfer used to end the copy silently,
  reporting a truncated file as success. With the cURL handler a stall or a premature close raises
  an exception that `BlobStorageRetryMiddleware` retries. The body is buffered in `php://temp`
  (memory up to 2 MB, then a temporary file) before it is copied to the destination, so a download
  briefly needs twice its size in disk space, as on GCP.
- The Azure upload client (`BlobClientFactory::createClientFromConnectionString()`) is unchanged:
  10 s connect timeout and a 120 s total deadline per request, i.e. per 4 MiB block
  (`ABSUploader::CHUNK_SIZE`) or per whole blob for a small single-request upload.
- The GCP policy is passed per download call (`GcsClientFactory::downloadOptions()`) rather than
  configured on the `StorageClient`, because client-level options never reach a download:
  `Rest::downloadObject()` always sets its own `restOptions`, and
  `RequestWrapper::getRequestOptions()` picks the per-request `restOptions` over the client-level
  ones with `??` instead of merging them.

## License

See [LICENSE](./LICENSE) file.
