<?php
error_reporting(E_ALL);
date_default_timezone_set('Europe/Prague');

define('STORAGE_API_URL', getenv('STORAGE_API_URL'));
define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN'));
define('STORAGE_API_LINKING_TOKEN', getenv('STORAGE_API_LINKING_TOKEN'));
define('STORAGE_API_GUEST_TOKEN', getenv('STORAGE_API_GUEST_TOKEN'));
define('STORAGE_API_READ_ONLY_TOKEN', getenv('STORAGE_API_READ_ONLY_TOKEN'));
define('STORAGE_API_SHARE_TOKEN', getenv('STORAGE_API_SHARE_TOKEN'));
// SOX TOKENS
define('STORAGE_API_DEFAULT_BRANCH_TOKEN', getenv('STORAGE_API_DEFAULT_BRANCH_TOKEN'));
define('STORAGE_API_REVIEWER_TOKEN', getenv('STORAGE_API_REVIEWER_TOKEN'));
define('STORAGE_API_DEVELOPER_TOKEN', getenv('STORAGE_API_DEVELOPER_TOKEN'));

define('STORAGE_API_MAINTENANCE_URL', getenv('STORAGE_API_MAINTENANCE_URL'));
define('STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION', getenv('STORAGE_API_TOKEN_ADMIN_2_IN_SAME_ORGANIZATION'));
define('STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION', getenv('STORAGE_API_TOKEN_ADMIN_3_IN_OTHER_ORGANIZATION'));
define('REDSHIFT_HOSTNAME', getenv('REDSHIFT_HOSTNAME'));
define('REDSHIFT_USER', getenv('REDSHIFT_USER'));
define('REDSHIFT_PASSWORD', getenv('REDSHIFT_PASSWORD'));
define('SUITE_NAME', getenv('SUITE_NAME'));
define('TRAVIS_BUILD_ID', getenv('TRAVIS_BUILD_ID'));
define('REDSHIFT_NODE_COUNT', getenv('REDSHIFT_NODE_COUNT'));

$revisionFilePath = realpath(__DIR__ . '/../REVISION');

if (file_exists($revisionFilePath)) {
    echo sprintf("Running tests revision: %s\n", file_get_contents($revisionFilePath));
}

require __DIR__ . '/../vendor/autoload.php';
