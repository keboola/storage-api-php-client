<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;

trait SnowflakeConnectionUtils
{
    public function ensureSnowflakeConnection(): Connection
    {
        static $connection = null;

        if ($connection === null) {
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

            $connection = SnowflakeConnectionFactory::getConnection($host, $user, $pass, $params);
        }

        return $connection;
    }

    public function getSnowflakeUser(): string
    {
        $user = getenv('SNOWFLAKE_USER');
        assert($user !== false, 'SNOWFLAKE_USER env var is not set');
        return $user;
    }

    private function getInboundSharedDatabaseName(): string
    {
        $inboundDatabaseName = getenv('SNOWFLAKE_INBOUND_DATABASE_NAME');
        assert($inboundDatabaseName !== false, 'SNOWFLAKE_INBOUND_DATABASE_NAME env var is not set');
        $this->assertCount(
            2,
            explode('.', $inboundDatabaseName),
            sprintf('SNOWFLAKE_INBOUND_DATABASE_NAME should have exactly 2 parts: <DATABASE_NAME>.<SCHEMA_NAME> gets %s', $inboundDatabaseName),
        );
        return $inboundDatabaseName;
    }

    private function getProducerSharedDatabase(): string
    {
        $producerDatabaseName = getenv('SNOWFLAKE_PRODUCER_SHARED_DATABASE');
        assert($producerDatabaseName !== false, 'SNOWFLAKE_PRODUCER_SHARED_DATABASE env var is not set');
        $this->assertCount(
            2,
            explode('.', $producerDatabaseName),
            sprintf('SNOWFLAKE_PRODUCER_SHARED_DATABASE should have exactly 2 parts: <DATABASE_NAME>.<SCHEMA_NAME> gets %s', $producerDatabaseName),
        );
        return $producerDatabaseName;
    }

    private function getProducerShareName(): string
    {
        $shareName = getenv('SNOWFLAKE_PRODUCER_SHARE_NAME');
        assert($shareName !== false, 'SNOWFLAKE_PRODUCER_SHARE_NAME env var is not set');
        return $shareName;
    }

    private function getProducerSnowflakeWarehouse(): string
    {
        $warehouse = getenv('SNOWFLAKE_PRODUCER_WAREHOUSE');
        assert($warehouse !== false, 'SNOWFLAKE_PRODUCER_WAREHOUSE env var is not set');
        return $warehouse;
    }

    private function grantImportedPrivilegesToProjectRole(string $projectRole): void
    {
        $db = $this->ensureSnowflakeConnection();
        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $db->executeQuery(sprintf(
            'GRANT IMPORTED PRIVILEGES ON DATABASE %s TO %s',
            explode('.', $this->getInboundSharedDatabaseName())[0],
            $projectRole,
        ));
    }

    private function ensureSharedDatabaseStillExists(): void
    {
        $db = $this->ensureSnowflakeConnection();
        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $database = $db->fetchAllAssociative(sprintf(
            'DESCRIBE DATABASE %s',
            explode('.', $this->getInboundSharedDatabaseName())[0],
        ));
        $this->assertNotEmpty($database);

        $tables = $db->fetchAllAssociative(sprintf(
            'SHOW TABLES IN %s',
            $this->getInboundSharedDatabaseName(),
        ));
        $this->assertCount(1, $tables);
        $this->assertSame('NAMES_TABLE', $tables[0]['name']);

        $views = $db->fetchAllAssociative(sprintf(
            'SHOW VIEWS IN %s',
            $this->getInboundSharedDatabaseName(),
        ));
        $this->assertCount(1, $views);
        $this->assertSame('SECURED_NAMES', $views[0]['name']);
    }

    private function ensureProducerSnowflakeConnection(): Connection
    {
        static $connection = null;

        if ($connection === null) {
            $host = getenv('SNOWFLAKE_PRODUCER_HOST');
            assert($host !== false, 'SNOWFLAKE_PRODUCER_HOST env var is not set');
            $user = getenv('SNOWFLAKE_PRODUCER_USER');
            assert($user !== false, 'SNOWFLAKE_PRODUCER_USER env var is not set');
            $pass = getenv('SNOWFLAKE_PRODUCER_PASSWORD');
            assert($pass !== false, 'SNOWFLAKE_PRODUCER_PASSWORD env var is not set');
            $warehouse = getenv('SNOWFLAKE_PRODUCER_WAREHOUSE');
            $params = [];
            if ($warehouse !== false) {
                $params['warehouse'] = $warehouse;
            }
            $connection = SnowflakeConnectionFactory::getConnection($host, $user, $pass, $params);
        }

        return $connection;
    }

    /**
     * @param string $tableName
     * @param string[] $columnsDefinition
     * @param array<array<mixed>> $data
     * @return void
     * @throws \Doctrine\DBAL\Exception
     */
    private function createTableInProducerDatabase(string $tableName, array $columnsDefinition, array $data): void
    {
        $db = $this->ensureProducerSnowflakeConnection();
        $dbName = $this->getProducerSharedDatabase();

        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $db->executeQuery(sprintf('USE WAREHOUSE %s', $this->getProducerSnowflakeWarehouse()));

        $db->executeQuery(sprintf(
            'CREATE TABLE IF NOT EXISTS %s.%s(%s)',
            $dbName,
            $tableName,
            implode(', ', $columnsDefinition),
        ));

        $db->executeQuery(sprintf(
            'TRUNCATE TABLE %s.%s',
            $dbName,
            $tableName,
        ));

        foreach ($data as $row) {
            $db->executeQuery(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                $dbName,
                $tableName,
                implode(', ', $row),
            ));
        }

        $db->executeQuery(sprintf(
            'GRANT SELECT ON TABLE %s.%s TO SHARE %s',
            $dbName,
            $tableName,
            $this->getProducerShareName(),
        ));
    }

    private function dropTableInProducerDatabase(string $tableName): void
    {
        $db = $this->ensureProducerSnowflakeConnection();
        $dbName = $this->getProducerSharedDatabase();

        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $db->executeQuery('USE WAREHOUSE DEV');

        $db->executeQuery(sprintf(
            'DROP TABLE IF EXISTS %s.%s',
            $dbName,
            $tableName,
        ));
    }

    private function createOrReplaceViewInProducerDatabase(string $viewName, string $selectStatement): void
    {
        $db = $this->ensureProducerSnowflakeConnection();
        $dbName = $this->getProducerSharedDatabase();

        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $db->executeQuery(sprintf('USE WAREHOUSE %s', $this->getProducerSnowflakeWarehouse()));

        $db->executeQuery(sprintf(
            'CREATE OR REPLACE SECURE VIEW %s.%s AS %s',
            $dbName,
            $viewName,
            $selectStatement,
        ));

        $db->executeQuery(sprintf(
            'GRANT SELECT ON VIEW %s.%s TO SHARE %s',
            $dbName,
            $viewName,
            $this->getProducerShareName(),
        ));
    }

    private function dropViewInProducerDatabase(string $viewName): void
    {
        $db = $this->ensureProducerSnowflakeConnection();
        $dbName = $this->getProducerSharedDatabase();

        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $db->executeQuery(sprintf('USE WAREHOUSE %s', $this->getProducerSnowflakeWarehouse()));

        $db->executeQuery(sprintf(
            'DROP VIEW IF EXISTS %s.%s',
            $dbName,
            $viewName,
        ));
    }
}
