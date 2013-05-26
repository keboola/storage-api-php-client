## 2.8.0
 * [BC-BREAK] translateApiErrors toggle removed - Client exception is thrown always from now
 * [Feature] SSL certificate validation
 * [Feature] Exponential backoff for curl errors and HTTP 5OO errors
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

