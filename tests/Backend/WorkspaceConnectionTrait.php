<?php

namespace Keboola\Test\Backend;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Test\StorageApiTestCase;

trait WorkspaceConnectionTrait
{
    /**
     * @return \Doctrine\DBAL\Connection|Connection|\PDO
     */
    private function getDbConnection(array $connection)
    {
        switch ($connection['backend']) {
            case StorageApiTestCase::BACKEND_SNOWFLAKE:
                $db = new Connection([
                    'host' => $connection['host'],
                    'database' => $connection['database'],
                    'warehouse' => $connection['warehouse'],
                    'user' => $connection['user'],
                    'password' => $connection['password'],
                ]);
                // set connection to use workspace schema
                $db->query(sprintf("USE SCHEMA %s;", $db->quoteIdentifier($connection['schema'])));

                return $db;
            case StorageApiTestCase::BACKEND_REDSHIFT:
                $pdo = new \PDO(
                    "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
                    $connection['user'],
                    $connection['password']
                );
                $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

                return $pdo;
            case StorageApiTestCase::BACKEND_SYNAPSE:
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

        throw new \Exception('Unsupported Backend for workspaces');
    }
}
