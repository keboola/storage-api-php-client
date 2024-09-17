<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;

trait ConnectionUtils
{
    private static ?Connection $connection = null;

    public function ensureSnowflakeConnection(): Connection
    {
        if (self::$connection === null) {
            $host = getenv('SNOWFLAKE_HOST');
            assert($host !== false, 'SNOWFLAKE_HOST env var is not set');
            $user = getenv('SNOWFLAKE_USER');
            assert($user !== false, 'SNOWFLAKE_USER env var is not set');
            $pass = getenv('SNOWFLAKE_PASSWORD');
            assert($pass !== false, 'SNOWFLAKE_PASSWORD env var is not set');
            $warehouse = getenv('SNOWFLAKE_WAREHOUSE');
            $params = [];
            if ($warehouse !== false) {
                $params['warehouse'] = $warehouse;
            }

            self::$connection = SnowflakeConnectionFactory::getConnection($host, $user, $pass, $params);
        }

        return self::$connection;
    }

    public function getSnowflakeUser(): string
    {
        $user = getenv('SNOWFLAKE_USER');
        assert($user !== false, 'SNOWFLAKE_USER env var is not set');
        return $user;
    }
}
