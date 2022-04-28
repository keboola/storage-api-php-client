<?php

namespace Keboola\Test\Backend;

use Doctrine\DBAL\Connection as DBALConnection;
use Keboola\Db\Import\Snowflake\Connection as SnowflakeConnection;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\Test\StorageApiTestCase;

trait WorkspaceConnectionTrait
{
    /**
     * @return SnowflakeConnection|DBALConnection|\PDO
     */
    private function getDbConnection(array $connection)
    {
        switch ($connection['backend']) {
            case StorageApiTestCase::BACKEND_SNOWFLAKE:
                return $this->getDbConnectionSnowflake($connection);
            case StorageApiTestCase::BACKEND_REDSHIFT:
                return $this->getDbConnectionRedshift($connection);
            case StorageApiTestCase::BACKEND_SYNAPSE:
                return $this->getDbConnectionSynapse($connection);
            case StorageApiTestCase::BACKEND_EXASOL:
                return $this->getDbConnectionExasol($connection);
        }

        throw new \Exception('Unsupported Backend for workspaces');
    }

    private function getDbConnectionSnowflake(array $connection): SnowflakeConnection
    {
        assert($connection['backend'] === StorageApiTestCase::BACKEND_SNOWFLAKE);
        $db = new SnowflakeConnection([
            'host' => $connection['host'],
            'database' => $connection['database'],
            'warehouse' => $connection['warehouse'],
            'user' => $connection['user'],
            'password' => $connection['password'],
        ]);
        // set connection to use workspace schema
        $db->query(sprintf("USE SCHEMA %s;", $db->quoteIdentifier($connection['schema'])));

        return $db;
    }

    private function getDbConnectionRedshift(array $connection): \PDO
    {
        assert($connection['backend'] === StorageApiTestCase::BACKEND_REDSHIFT);
        $pdo = new \PDO(
            "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
            $connection['user'],
            $connection['password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    private function getDbConnectionSynapse(array $connection): DBALConnection
    {
        assert($connection['backend'] === StorageApiTestCase::BACKEND_SYNAPSE);
        $db = \Doctrine\DBAL\DriverManager::getConnection([
            'user' => $connection['user'],
            'password' => $connection['password'],
            'host' => $connection['host'],
            'dbname' => $connection['database'],
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
            'driverOptions' => [
                'LoginTimeout' => 30,
                'ConnectRetryCount' => 5,
                'ConnectRetryInterval' => 10,
            ],
        ]);
        $db->connect();

        return $db;
    }

    private function getDbConnectionExasol(array $connection): DBALConnection
    {
        assert($connection['backend'] === StorageApiTestCase::BACKEND_EXASOL);
        $db = ExasolConnectionFactory::getConnection(
            $connection['host'],
            $connection['user'],
            $connection['password']
        );
        $db->executeStatement(sprintf(
            'OPEN SCHEMA %s',
            ExasolQuote::quoteSingleIdentifier($connection['schema'])
        ));

        return $db;
    }
}
