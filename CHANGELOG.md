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

