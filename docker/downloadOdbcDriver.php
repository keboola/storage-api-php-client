<?php
/**
 * Loads test fixtures into S3
 */

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', true);
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/vendor/autoload.php';

$client =  new \Aws\S3\S3Client([
    'region' => 'us-east-1',
    'version' => '2006-03-01',
    'credentials' => [
        'key' => getenv('AWS_ACCESS_KEY'),
        'secret' => getenv('AWS_SECRET_KEY'),
    ],
]);


$client->getObject([
    'Bucket' => 'keboola-configs',
    'Key' => 'drivers/snowflake/snowflake_linux_x8664_odbc.2.12.73.tgz',
    'SaveAs' => './snowflake_linux_x8664_odbc.tgz'
]);
