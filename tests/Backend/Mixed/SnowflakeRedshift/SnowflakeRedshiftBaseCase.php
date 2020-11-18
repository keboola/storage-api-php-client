<?php

namespace Keboola\Test\Backend\Mixed\SnowflakeRedshift;

use Keboola\Test\Backend\Mixed\StorageApiSharingTestCase;

abstract class SnowflakeRedshiftBaseCase extends StorageApiSharingTestCase
{
    public function sharingBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT],
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT],
        ];
    }
}
