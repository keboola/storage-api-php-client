<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Doctrine\DBAL\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ConnectionUtils;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;

class SnowflakeRegisterExternalBucketInSecureDataShareTest extends StorageApiTestCase
{
    use ConnectionUtils;
    use EventTesterUtils;

    public function setUp(): void
    {
        parent::setUp();
        $this->allowTestForBackendsOnly([self::BACKEND_SNOWFLAKE], 'Backend has to support external buckets');
    }

    public function testRegisterExternalBucket(): void
    {
        $externalTableNames = [
            'NAMES_TABLE',
            'SECURED_NAMES',
        ];

        $this->initEvents($this->_client);
        $runId = $this->setRunId();

        $workspaces = new Workspaces($this->_client);
        $workspace0 = $workspaces->createWorkspace(['backend' => 'snowflake']);
        $projectRole = $workspace0['connection']['database'];

        $this->grantImportedPrivilegesToProjectRole($projectRole);

        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        $this->_client->registerBucket(
            $testBucketName,
            explode('.', $this->getInboundSharedDatabaseName()),
            self::STAGE_IN,
            $description,
            'snowflake',
            null,
            true,
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $registeredBucket = $this->_client->getBucket($bucketId);

        $this->assertSame($testBucketName, $registeredBucket['name']);
        $this->assertSame(self::STAGE_IN, $registeredBucket['stage']);
        $this->assertSame(
            sprintf('%s.%s', $projectRole, 'SDS_'.mb_strtoupper(str_replace('-', '_', $testBucketName))),
            $registeredBucket['path'],
        );
        $this->assertTrue($registeredBucket['isSnowflakeSharedDatabase']);

        $registeredTableNames = [];
        foreach ($registeredBucket['tables'] as $table) {
            $registeredTableNames[] = $table['name'];
        }

        $this->assertEquals($externalTableNames, $registeredTableNames, 'Not all external tables/views have registered view.');

        $this->_client->dropBucket($bucketId);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketDeleted')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $bucketExist = $this->_client->bucketExists($bucketId);
        $this->assertFalse($bucketExist, 'Bucket '.$bucketId.' still exist.');

        $this->ensureSharedDatabaseStillExists();
    }

    public function testInvalidDbToRegister(): void
    {
        $bucketName = 'test-sds-bucket';
        $bucketId = self::STAGE_IN.'.'.$bucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        try {
            $this->_client->registerBucket(
                $bucketName,
                ['non-existing-database', 'non-existing-schema'],
                self::STAGE_IN,
                'will fail',
                'snowflake',
                'test-bucket-will-fail',
                true,
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.dbObjectNotFound', $e->getStringCode(), $e->getMessage());
            $this->assertStringContainsString(
                'doesn\'t exist or project user is missing privileges to read from it.',
                $e->getMessage(),
            );
        }
    }

    public function testRegisterAndRefreshExternalBucket(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace0 = $workspaces->createWorkspace(['backend' => 'snowflake']);
        $projectRole = $workspace0['connection']['database'];

        $newTableName = 'NEW_TABLE';
        $this->dropTableInProducerDatabase($newTableName);
        $this->grantImportedPrivilegesToProjectRole($projectRole);

        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        $this->_client->registerBucket(
            $testBucketName,
            explode('.', $this->getInboundSharedDatabaseName()),
            self::STAGE_IN,
            $description,
            'snowflake',
            null,
            true,
        );

        $registeredBucket = $this->_client->getBucket($bucketId);
        $this->assertTrue($registeredBucket['isSnowflakeSharedDatabase']);

        $this->createTableInProducerDatabase($newTableName, ['id INT', 'name VARCHAR'], [[1, "'Jan'"], [2, "'Josef'"]]);
        $this->_client->refreshBucket($registeredBucket['id']);

        $refreshedBucket = $this->_client->getBucket($registeredBucket['id']);
        $tableNames = array_map(
            function ($tableRow) {
                return $tableRow['name'];
            },
            $refreshedBucket['tables'],
        );
        $this->assertTrue(in_array($newTableName, $tableNames), 'New table not found in refreshed bucket');

        $this->_client->dropBucket($bucketId);
        $bucketExist = $this->_client->bucketExists($bucketId);
        $this->assertFalse($bucketExist, 'Bucket '.$bucketId.' still exist.');
        $this->dropTableInProducerDatabase($newTableName);
        $this->ensureSharedDatabaseStillExists();
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

    protected function setRunId(): string
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        return $runId;
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
}
