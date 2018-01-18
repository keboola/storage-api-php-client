<?php
error_reporting(E_ALL);
date_default_timezone_set('Europe/Prague');

define('STORAGE_API_URL', getenv('STORAGE_API_URL'));
define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN'));
define('STORAGE_API_LINKING_TOKEN', getenv('STORAGE_API_LINKING_TOKEN'));
define('STORAGE_API_MAINTENANCE_URL', getenv('STORAGE_API_MAINTENANCE_URL'));
define('REDSHIFT_HOSTNAME', getenv('REDSHIFT_HOSTNAME'));
define('REDSHIFT_USER', getenv('REDSHIFT_USER'));
define('REDSHIFT_PASSWORD', getenv('REDSHIFT_PASSWORD'));

$revisionFilePath = realpath(__DIR__ . '/../REVISION');

if (file_exists($revisionFilePath)) {
    echo sprintf("Running tests revision: %s\n", file_get_contents($revisionFilePath));
}

require __DIR__ . '/../vendor/autoload.php';
