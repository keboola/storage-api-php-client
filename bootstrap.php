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

require_once 'config.php';
require_once 'src/Keboola/StorageApi/Exception.php';
require_once 'src/Keboola/StorageApi/Client.php';
require_once 'src/Keboola/StorageApi/ClientException.php';
require_once 'src/Keboola/StorageApi/OneLiner.php';
require_once 'src/Keboola/StorageApi/OneLinerException.php';