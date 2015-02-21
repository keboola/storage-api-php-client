## 2.12.7 (2015-02-21)
 * [Fix] `TableExporter` fixed columns parameter handling
 * [Fix] `TableExporter` fix in default format used for header composition for sliced files

## 2.12.6 (2015-02-21)
 * [Fix] revert of changes from 2.12.5

## 2.12.5 (2015-02-20)
  * [Fix] Forward `columns` attribute in export option in `TableExporter`
  * [Fix] Fix phpdoc in `TableExporter`

## 2.12.4 (2015-01-24)
  * [Fix] close file commands

## 2.12.3 (2015-01-23)
  * [Fix] added close file commands for windows compatibility
  * [Fix] added error checking in Table constructor

## 2.12.2 (2015-01-20)
  * [Feat] Redshift aliases supported

## 2.12.1 (2015-01-09)
   * [Feat] `Components` - component configuration JSON storage

## 2.12.0 (2014-12-30)
   * [BC break] `file uploads` files are encrypted by default. `x-amz-server-side-encryption` header has to be added to direct uploads to S3, if `uploadFile` method is used nothing has to be changed.

## 2.11.28 (2014-12-30)
   * [Feat] `file uploads` encryption support

## 2.11.27 (2014-11-12)
   * [Feat] `generateRunId` method added, support for runId hieararchy

## 2.11.26 (2014-11-10)
  * [Fix] allow guzzlehttp 4.0 =< 5 (4.1.2 is buggy)

## 2.11.25 (2014-11-10)
  * [Fix] Client version

## 2.11.24 (2014-10-21)
  * [Feat] List Files `maxId` and `sinceId` parameters added

## 2.11.23 (2014-10-29)
  * [Feat] Bucket credentials resource support added

## 2.11.22 (2014-10-21)
  * [Feat] Client version in user agent
  * [Fix] file upload - S3 error response body

## 2.11.21 (2014-10-20)
  * [Fix] tableExporter - remove SSL v3

## 2.11.20 (2014-09-16)
  * [Feat] Components - get configuration method added

## 2.11.19 (2014-09-16)
  * [Feat] Components resource added

## 2.11.18 (2014-09-02)
  * [Feat] File uploads - don't notify by default

## 2.11.17 (2014-07-24)
  * [Fix] Table exporter - process timeout

## 2.11.16 (2014-07-17)
  * [Feat] Delete file from file uploads

## 2.11.15 (2014-07-16)
  * [Fix] Handling of non-json response from SAPI
  * [Fix] Redshift escaped format

## 2.11.14 (2014-07-14)
  * [Fix] TableExporter - Symfony 2.3 compatibility

## 2.11.13 (2014-07-08)
  * [Fix] TableExporter - overwrite existing destination file

## 2.11.12 (2014-07-08)
  * [Improvement] Allow Symfony components from 2.3

## 2.11.11 (2014-06-26)
  * [Feat] Event subsriber
  * [Bugfix] Table class - csv file delimiter and enclosure

## 2.11.10 (2014-06-17)
  * [Improvement] TableExporter - use custom S3 backoff
  * [Bugfix] Tests - allow listObjects with prefix on sliced async exports
  * [Bugfix] Tests - redshift export fix

## 2.11.9 (2014-06-11)
  * [Bugfix] TableExporter - respect output format when composing csv file header

## 2.11.8 (2014-06-10)
  * [Improvement] Modify S3Client CURL options

## 2.11.7 (2014-06-10)
  * [Feature] Client method `createTableAsyncDirect` added

## 2.11.6 (2014-06-06)
  * [Feature] TableExporter class to export sliced tables to a single CSV file.

## 2.11.5 (2014-05-30)
  * [Feature] Create bucket - backend type can be set by parameter. Allowed types are `mysql` and `redshift`, `mysql` is default.

## 2.11.4 (2014-05-27)
  * [Improvement] Symfony components updated to 2.4.

## 2.11.3 (2014-05-15)
  * [Improvement] Removed timeout on async tasks

## 2.11.2 (2014-05-13)

  * [Improvement] Use different exception class for async job timeouts

## 2.11.1 (2014-04-22)
  * [Bugfix] Exception handling of case when temporary files cannot be opened

## 2.11.0 (2014-04-17)

HTTP backend migrated to Guzzle 4.0.

### BC Breaks
  * Constructor accepts configuration array instead of list of parameters
  * Following setters removed, these can be set only from constructor from now:
    * `setApiUrl`
    * `setUserAgent`
    * `setBackoffMaxTries`
  * Changes in logging
    * Static logging callback removed
    * Logger must be set in constructor:

```php
     $client = new Keboola\StorageApi\Client(array(
        'token' => STORAGE_API_TOKEN,
        'logger' => new \GuzzleHttp\Subscriber\Log\SimpleLogger(function($message) {
            echo $message . PHP_EOL;
        })
      ));
```

## 2.10.2 (2014-04-16)
 * [Refactoring] Guzzle upgraded to 3.9

## 2.10.1 (2014-03-05)
 * [Feature] Table import - columns parameter
 * [Deprecation] Table import - withoutHeaders parameter deprecated

## 2.10.0 (2014-02-26)
 * [Feature] File uploads - sliced files support
 * [Feature] Table import - sliced files can be imported
 * [Feature] Table import - files without headers can be imported
 * [BC break] Verify token is not called in contstructor


## 2.9.3 (2014-02-21)
 * [Bugfix] File uploads - gzipped folder deleted when running two uploads simultaneously

## 2.9.2 (2014-02-21)
 * [Bugfix] handleAsyncTask - not waiting after success
 * [Feature] handleAsyncTask - wait max. 30 seconds

## 2.9.1
 * [Performance] bucketExists and tableExists methods use get resource instead of list

## 2.9.0
 * [Feature] Asynchronous table export
 * [Feature] File uploads - lifecycle settings `isPermanent` parameter
 * [Feature] File uploads - federation token for better integration with AWS services
 * [Feature] File uploads - tagging and search support
 * [BC Break] File uploads - methods accepts options objects instead of arrays or single parameters
 * [BC Break] Most of protected methods and properties switched to private
 * [BC Break] Code style - removed underscore prefixes from private and protected methods and properties

## 2.8.30
 * Keboola/csv patch version unlocked from 1.1.3

## 2.8.29
 * [Bugfix] File upload - compress detection switched to suffix comparation

## 2.8.28
 * [Change] Default timeout increased from 30 minutes to 2 hours
 * [Bugfix] Added check if gzip compression succeded. If failed, then the resulting error message was "File sizeBytes must be set" because gziped file was zero size
 * [Bugfix] Forced closing of temporary file in Table::save()

## 2.8.27
 * [Bugfix] Maintenance response missing reason
 * [Feature] Automatic compression of CSV files imported asynchronously

## 2.8.26
 * [Bugfix] Reading CSV on Windows

## 2.8.25
 * [Bugfix] Short open tags removed

## 2.8.24
 * [Improvement] Asynchronous task timeout message more descriptive

## 2.8.23
 * [Bugfix] Default exponential backoff increased to 11 retries (30 min)
 * [Bugfix] Tests - default urls changed

## 2.8.22
 * [Bugfix] Backoff - added retry on curl error 18 (CURLE_PARTIAL_FILE)

## 2.8.21
 * [Feature] `getComponents` method - returns parsed data from indexAction

## 2.8.20
 * [Feature] Added getter for UserAgent

## 2.8.19
 * [Feature] Partial responses support in list methods

## 2.8.18
 * [Feature] Index method returning information about Keboola Connection stack

## 2.8.17
 * [Feature] Table rows delete

## 2.8.16
 * [Feature] Table rollback from snapshot

## 2.8.15
 * [Feature] Snapshotting support init

## 2.8.14
 * [Feature] `ListTableEvents` method added

## 2.8.13
 * [Feature] Set exponential backoff maximum tries number in constructor
 * [Feature] Exponencial backoff also for maintenance

## 2.8.12
 * [Refactoring] Guzzle upgraded to 3.7.*

## 2.8.11
 * [Refactoring] Create table async refactored to use new create-async table method

## 2.8.10
 * [Refactoring] Automatic async tasks handling

## 2.8.9
 * [Bugfix] Async import method outpur compatible with sync import method, exception is thrown when job finishes with error

## 2.8.8
 * [Feature] Table class method save() has now optional $async parameter (default: $async = false)

## 2.8.7
 * [Bugfix] Tests bugfixes

## 2.8.6
 * [Feature] Token share by email method added

## 2.8.5
 * [Feature] Async table create method added

## 2.8.4
 * [Feature] File upload prepare - send file size

## 2.8.3
 * [Feature] Async table import

## 2.8.2
 * [Refactoring] File uploads directly to S3 (http://docs.keboola.apiary.io/#post-%2Fv2%2Fstorage%2Ffiles%2Fprepare)

## 2.8.1
 * [Bugfix] Guzzle version fixed to 3.6.0. + upgrade related issues

## 2.8.0
 * [BC break] translateApiErrors toggle removed - Client exception is thrown always from now
 * [Feature] SSL certificate validation
 * [Feature] Exponential backoff for curl errors and HTTP 500 errors
 * [Feature] All HTTP requests gzipped
 * [Refactoring] HTTP access migrated from raw curl to Guzzle

## 2.7.3
 * [Bugfix] cUrl post without data caused Request Entity Too Large error

## 2.7.2
 * [Feature] Events filtering

## 2.7.1
 * [Feature] Alias filter
 * [Feature] Alias columns autosync enable/disable

## 2.7.0

 * [BC break] writeTable, createTable methods arguments list switched to options array
 * [BC break] getTableDefinition method removed
 * [Feature] writeTable, createTable - CSV escaped by parameter added

## 2.6.17
 * [Feature] Table export formats: `rfc`, `escaped`, `raw`
