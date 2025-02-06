<?php

namespace Keboola\Test\Backend;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Keboola\TableBackendUtils\Connection\Bigquery\Retry;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Doctrine\DBAL\Connection as DBALConnection;
use Keboola\Db\Import\Snowflake\Connection as SnowflakeConnection;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\Test\StorageApiTestCase;
use Psr\Log\NullLogger;

trait WorkspaceConnectionTrait
{
    /**
     * @return SnowflakeConnection|DBALConnection|\PDO|BigQueryClient
     */
    private function getDbConnection(array $connection, int $retriesCount = 20)
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
            case StorageApiTestCase::BACKEND_TERADATA:
                return $this->getDbConnectionTeradata($connection);
            case StorageApiTestCase::BACKEND_BIGQUERY:
                return $this->getDbConnectionBigquery($connection, $retriesCount);
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
        $db->query(sprintf('USE SCHEMA %s;', $db->quoteIdentifier($connection['schema'])));

        return $db;
    }

    public function getDbConnectionSnowflakeDBAL(array $connection): DBALConnection
    {
        assert($connection['backend'] === StorageApiTestCase::BACKEND_SNOWFLAKE);
        $db = SnowflakeConnectionFactory::getConnection(
            $connection['host'],
            $connection['user'],
            $connection['password'],
            [
                'database' => $connection['database'],
                'warehouse' => $connection['warehouse'],
            ],
        );
        // set connection to use workspace schema
        $db->executeStatement(sprintf(
            'USE SCHEMA %s;',
            $db->quoteIdentifier($connection['schema']),
        ));

        return $db;
    }

    private function getDbConnectionRedshift(array $connection): \PDO
    {
        assert($connection['backend'] === StorageApiTestCase::BACKEND_REDSHIFT);
        $pdo = new \PDO(
            "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
            $connection['user'],
            $connection['password'],
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
            $connection['password'],
        );
        $db->connect();
        $db->executeStatement(sprintf(
            'OPEN SCHEMA %s',
            ExasolQuote::quoteSingleIdentifier($connection['schema']),
        ));

        return $db;
    }

    private function getDbConnectionTeradata(array $connection): DBALConnection
    {
        $db = TeradataConnection::getConnection([
            'host' => $connection['host'],
            'port' => 1025,
            'user' => $connection['user'],
            'password' => $connection['password'],
            'dbname' => $connection['schema'],
        ]);
        $db->connect();
        $db->executeStatement(sprintf(
            'SET SESSION DATABASE %s',
            TeradataQuote::quoteSingleIdentifier($connection['schema']),
        ));
        $db->executeStatement('SET ROLE ALL');

        return $db;
    }

    private function getDbConnectionBigquery(array $connection, int $retriesCount): BigQueryClient
    {
        // note: the close method is not used in this client
        $bqClient = new BigQueryClientWrapper([
            'keyFile' => $connection['credentials'],
            'restRetryFunction' => Retry::getRestRetryFunction(new NullLogger()),
            'requestTimeout' => 120,
            'retries' => $retriesCount,
        ], 'sapitest');

        $bqClient->runQuery(
            $bqClient->query('SELECT SESSION_USER() AS USER'),
        )->getIterator()->current();

        return $bqClient;
    }
}
