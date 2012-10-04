<?php
// Define path to application directory
define('ROOT_PATH', __DIR__);

// Ensure library/ is on include_path
set_include_path(implode(PATH_SEPARATOR, array(
	realpath(ROOT_PATH . '/library'),
	get_include_path(),
)));
ini_set('display_errors', true);

date_default_timezone_set('Europe/Prague');

if (file_exists('config.php')) {
	require_once 'config.php';
}

defined('STORAGE_API_URL')
	|| define('STORAGE_API_URL', getenv('STORAGE_API_URL') ? getenv('STORAGE_API_URL') : 'https://connection-devel.keboola.com');

defined('STORAGE_API_TOKEN')
	|| define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN') ? getenv('STORAGE_API_TOKEN') : 'your_token');

require_once 'tests/Test/StorageApiTestCase.php';
require_once 'src/Keboola/StorageApi/Exception.php';
require_once 'src/Keboola/StorageApi/Client.php';
require_once 'src/Keboola/StorageApi/ClientException.php';
require_once 'src/Keboola/StorageApi/OneLiner.php';
require_once 'src/Keboola/StorageApi/OneLinerException.php';
require_once 'src/Keboola/StorageApi/Config/Reader.php';
require_once 'src/Keboola/StorageApi/Config/Exception.php';
require_once 'src/Keboola/StorageApi/Table.php';
require_once 'src/Keboola/StorageApi/TableException.php';

require_once ROOT_PATH . '/vendor/autoload.php';
