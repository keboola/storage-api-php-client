<?php
// Define path to application directory
define('ROOT_PATH', __DIR__);

// Ensure library/ is on include_path
set_include_path(implode(PATH_SEPARATOR, array(
	realpath(ROOT_PATH . '/library'),
	get_include_path(),
)));
ini_set('display_errors', true);
error_reporting(E_ALL);

date_default_timezone_set('Europe/Prague');

if (file_exists(__DIR__ . '/config.php')) {
	require_once __DIR__ . '/config.php';
}

defined('STORAGE_API_URL')
	|| define('STORAGE_API_URL', getenv('STORAGE_API_URL') ? getenv('STORAGE_API_URL') : 'https://connection.keboola.com');

defined('STORAGE_API_TOKEN')
	|| define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN') ? getenv('STORAGE_API_TOKEN') : 'your_token');

defined('STORAGE_API_MAINTENANCE_URL')
	|| define('STORAGE_API_MAINTENANCE_URL', getenv('STORAGE_API_MAINTENANCE_URL') ? getenv('STORAGE_API_MAINTENANCE_URL') : 'https://maintenance-testing.keboola.com/');


defined('REDSHIFT_HOSTNAME')
|| define('REDSHIFT_HOSTNAME', getenv('REDSHIFT_HOSTNAME') ? getenv('REDSHIFT_HOSTNAME') : '');

defined('REDSHIFT_USER')
|| define('REDSHIFT_USER', getenv('REDSHIFT_USER') ? getenv('REDSHIFT_USER') : '');

defined('REDSHIFT_PASSWORD')
|| define('REDSHIFT_PASSWORD', getenv('REDSHIFT_PASSWORD') ? getenv('REDSHIFT_PASSWORD') : '');


require_once 'tests/Test/StorageApiTestCase.php';
require_once ROOT_PATH . '/vendor/autoload.php';
