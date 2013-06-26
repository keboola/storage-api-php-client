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

