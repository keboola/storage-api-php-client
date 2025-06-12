<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\ExternalBuckets;

use Doctrine\DBAL\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ConnectionUtils;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;
use Throwable;

class SnowflakeRegisterExternalBucketInSecureDataShareTest extends StorageApiTestCase
{
    use ConnectionUtils;
    use EventTesterUtils;

    protected Client $shareClient;

    protected Client $linkingClient;

    public function setUp(): void
    {
        parent::setUp();
        $this->allowTestForBackendsOnly([self::BACKEND_SNOWFLAKE], 'Backend has to support external buckets');

        $this->shareClient = $this->getClientForToken(
            STORAGE_API_SHARE_TOKEN,
        );
        $this->linkingClient = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );

        $tokenData = $this->shareClient->verifyToken();
        if ($tokenData['organization']['id'] !== $this->linkingClient->verifyToken()['organization']['id']) {
            $this->fail('STORAGE_API_LINKING_TOKEN is not in the same organization as STORAGE_API_TOKEN');
        }

        // added in testLoadIntoExternalBucketWithOutdatedViewReturnsMeaningfulError
        $this->dropViewInProducerDatabase('TEMP_VIEW_FOR_LOAD');
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

        $workspaces->deleteWorkspace($workspace0['id']);
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

        $workspaces->deleteWorkspace($workspace0['id']);
    }

    public function testLoadIntoExternalBucketWithOutdatedViewReturnsMeaningfulError(): void
    {
        // Phase 1: Initial Setup
        $viewName = 'TEMP_VIEW_FOR_LOAD';
        [$producerDb, $producerSchema] = explode('.', $this->getProducerSharedDatabase());
        $inboundShareDbName = $this->getInboundSharedDatabaseName();
        $db = $this->ensureProducerSnowflakeConnection(); // Get producer connection

        // Use a fully qualified name for the table in the view definition
        $initialSelect = sprintf(
            'SELECT "ID", "NAME" FROM %s.%s."NAMES_TABLE"',
            $db->quoteIdentifier($producerDb),
            $db->quoteIdentifier($producerSchema),
        );

        $this->dropViewInProducerDatabase($viewName);
        $this->createOrReplaceViewInProducerDatabase($viewName, $initialSelect);

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(['backend' => 'snowflake']);
        $projectRole = $workspace['connection']['database'];

        $this->grantImportedPrivilegesToProjectRole($projectRole);

        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_client, $bucketId);

        $this->_client->registerBucket(
            $testBucketName,
            explode('.', $inboundShareDbName),
            self::STAGE_IN,
            $description,
            'snowflake',
            null,
            true,
        );

        $registeredBucket = $this->_client->getBucket($bucketId);
        $this->assertTrue($registeredBucket['isSnowflakeSharedDatabase']);

        $tables = $this->_client->listTables($bucketId);
        $foundTable = false;
        foreach ($tables as $table) {
            if ($table['name'] === $viewName) {
                $foundTable = true;
                break;
            }
        }
        $this->assertTrue($foundTable, sprintf('Table for view "%s" not found in registered bucket.', $viewName));

        // Phase 2: Alter View & Test Load Failure
        $alteredSelect = sprintf(
            'SELECT "ID", "NAME", CURRENT_TIMESTAMP() AS "LOAD_TS" FROM %s.%s."NAMES_TABLE"',
            $db->quoteIdentifier($producerDb),
            $db->quoteIdentifier($producerSchema),
        );
        $this->createOrReplaceViewInProducerDatabase($viewName, $alteredSelect);

        // Find the table ID corresponding to the view in the registered bucket
        $tables = $this->_client->listTables($bucketId);
        $sourceTableId = null;
        foreach ($tables as $table) {
            if ($table['name'] === $viewName) {
                $sourceTableId = $table['id'];
                break;
            }
        }
        $this->assertNotNull($sourceTableId, 'Could not find source table ID for view.');
        try {
            $workspaces->loadWorkspaceData($workspace['id'], [
                'input' => [
                    [
                        'source' => $sourceTableId,
                        'destination' => 'destination_fail_ws',
                        // We don't specify columns, expecting it to fail on schema mismatch
                    ],
                ],
            ]);
            $this->fail('Load from outdated view schema should have failed.');
        } catch (ClientException $e) {
            $this->assertStringContainsStringIgnoringCase('External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.', $e->getMessage());
        }

        // Phase 2.5: Test Data Preview and Export Failure
        try {
            $this->_client->getTableDataPreview($sourceTableId);
            $this->fail('Data preview with outdated view schema should have failed.');
        } catch (ClientException $e) {
            // Expecting the same error as workspace load
            $this->assertStringContainsStringIgnoringCase('External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.', $e->getMessage());
        }

        try {
            $this->_client->exportTableAsync($sourceTableId);
            $this->fail('Export job should have failed for outdated view');
        } catch (ClientException $e) {
            $this->assertStringContainsStringIgnoringCase(
                'External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.',
                $e->getMessage(),
            );
        }

        // Phase 3: Refresh Bucket & Test Load Success
        $this->_client->refreshBucket($bucketId);

        // Verify metadata was updated after refresh
        $refreshedTable = $this->_client->getTable($sourceTableId);
        $this->assertCount(3, $refreshedTable['columns']);
        $this->assertContains('LOAD_TS', $refreshedTable['columns']);

        // Attempt workspace load again, should succeed now
        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'destination_success_ws',
                ],
            ],
        ]);

        // Verify data in workspace
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $data = $backend->fetchAll('destination_success_ws');
        $this->assertCount(5, $data);

        // Assert based on numerical indices
        $this->assertCount(3, $data[0]); // Check for 3 columns
        $this->assertIsNumeric($data[0][0]); // ID
        $this->assertIsString($data[0][1]); // NAME
        $this->assertIsString($data[0][2]); // LOAD_TS (timestamp as string)
        $this->assertNotEmpty($data[0][2]); // Ensure timestamp is not empty

        // Phase 4: Cleanup
        $this->dropViewInProducerDatabase($viewName);
        $this->_client->dropBucket($bucketId, ['force' => true]); // Force drop

        $bucketExists = $this->_client->bucketExists($bucketId);
        $this->assertFalse($bucketExists, 'Bucket ' . $bucketId . ' should have been dropped.');
    }

    public function testRefreshBucketInLinkedProject(): void
    {
        $stage = self::STAGE_IN;
        $description = $this->generateDescriptionForTestObject();
        $bucketName = $this->getTestBucketName($description);
        $newTableName = 'NEW_TABLE';
        $bucketId = $stage . '.' . $bucketName;

        $this->forceUnshareBucketIfExists($this->shareClient, $stage . '.' . $bucketName, true);
        $this->dropBucketIfExists($this->_client, $stage.'.'.$bucketName, true);
        $this->dropTableInProducerDatabase($newTableName);

        $workspaces = new Workspaces($this->_client);
        $workspace0 = $workspaces->createWorkspace(['backend' => 'snowflake']);
        $projectRole = $workspace0['connection']['database'];

        $this->grantImportedPrivilegesToProjectRole($projectRole);

        $this->dropBucketIfExists($this->_client, $bucketId);

        $this->_client->registerBucket(
            $bucketName,
            explode('.', $this->getInboundSharedDatabaseName()),
            self::STAGE_IN,
            $description,
            'snowflake',
            null,
            true,
        );

        $tables = $this->_client->listTables($stage.'.'.$bucketName);
        // tables created manually during setup
        $this->assertCount(2, $tables);

        $shareToken = $this->linkingClient->verifyToken();
        $targetProjectId = $shareToken['owner']['id'];

        $this->shareClient->shareBucketToProjects($bucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertEquals($targetProjectId, $sharedBucket['sharingParameters']['projects'][0]['id']);

        $linkingWorkspaces = new Workspaces($this->linkingClient);
        $linkingWorkspace = $linkingWorkspaces->createWorkspace([], true);
        $linkingBackend = WorkspaceBackendFactory::createWorkspaceBackend($linkingWorkspace);

        /** @var \Keboola\Db\Import\Snowflake\Connection $linkingSnowflakeDb */
        $linkingSnowflakeDb = $linkingBackend->getDb();

        // check before link is not work via RO
        try {
            $linkingSnowflakeDb->fetchAll(sprintf(
                'SELECT * FROM %s.%s',
                $sharedBucket['path'],
                'NAMES_TABLE', // table created manually during setup
            ));
            $this->fail('Select should fail.');
        } catch (Throwable $e) {
            $dbName = explode('.', $sharedBucket['path'])[0];
            $this->assertStringContainsString("Database '{$dbName}' does not exist or not authorized., SQL state 02000 in SQLPrepare", $e->getMessage());
        }

        // LINKING START

        $token = $this->_client->verifyToken();
        $linkedBucketId = $this->linkingClient->linkBucket('LINKED_BUCKET', self::STAGE_IN, $token['owner']['id'], $sharedBucket['id'], 'LINKED_BUCKET');
        $linkedBucket = $this->linkingClient->getBucket($linkedBucketId);
        $this->assertEquals($sharedBucket['id'], $linkedBucket['sourceBucket']['id']);

        $linkingTables = $this->linkingClient->listTables($linkedBucketId);
        $this->assertCount(2, $linkingTables);
        $linkingTable = $linkingTables[0];

        $dataPreview = $this->linkingClient->getTableDataPreview($linkingTable['id'], ['columns' => ['ID', 'NAME']]);
        $this->assertEquals(
            <<<EXPECTED
"ID","NAME"
"1","Jiří"
"2","Roman"
"3","Tomáš"
"4","Vojta"
"5","Martin"

EXPECTED,
            $dataPreview,
        );

        // test RO works
        /** @var \Keboola\Db\Import\Snowflake\Connection $linkingSnowflakeDb */
        $linkingSnowflakeDb = $linkingBackend->getDb();

        $result = $linkingSnowflakeDb->fetchAll(sprintf(
            'SELECT * FROM %s.%s',
            $linkedBucket['path'],
            'NAMES_TABLE', // table created manually during setup
        ));
        $this->assertEquals(
            [
                [
                    'ID' => 1,
                    'NAME' => 'Jiří',
                ],
                [
                    'ID' => 2,
                    'NAME' => 'Roman',
                ],
                [
                    'ID' => 3,
                    'NAME' => 'Tomáš',
                ],
                [
                    'ID' => 4,
                    'NAME' => 'Vojta',
                ],
                [
                    'ID' => 5,
                    'NAME' => 'Martin',
                ],
            ],
            $result,
        );

        // REFRESH START

        $this->createTableInProducerDatabase($newTableName, ['ID INT', 'NAME VARCHAR'], [[1, "'Jan'"], [2, "'Josef'"]]);
        $this->_client->refreshBucket($bucketId);

        $refreshedBucket = $this->_client->getBucket($bucketId);
        $tableNames = array_map(
            function ($tableRow) {
                return $tableRow['name'];
            },
            $refreshedBucket['tables'],
        );
        $this->assertTrue(in_array($newTableName, $tableNames), 'New table not found in refreshed bucket');

        $linkingTables = $this->linkingClient->listTables($linkedBucketId);
        $this->assertCount(3, $linkingTables);

        $linkedBucket = $this->linkingClient->getBucket($linkedBucketId);
        $linkedTableNames = array_map(
            function ($tableRow) {
                return $tableRow['name'];
            },
            $linkedBucket['tables'],
        );
        $this->assertTrue(in_array($newTableName, $linkedTableNames), 'New table not found in linked bucket after refresh');

        $filteredTables = array_filter(
            $linkedBucket['tables'],
            function ($tableRow) use ($newTableName) {
                return $tableRow['name'] === $newTableName;
            },
        );

        $linkedTable = reset($filteredTables);

        $dataPreview = $this->linkingClient->getTableDataPreview($linkedTable['id'], ['columns' => ['ID', 'NAME']]);
        $this->assertEquals(
            <<<EXPECTED
"ID","NAME"
"1","Jan"
"2","Josef"

EXPECTED,
            $dataPreview,
        );

        // test RO works
        $result = $linkingSnowflakeDb->fetchAll(sprintf(
            'SELECT * FROM %s.%s',
            $linkedBucket['path'],
            $newTableName,
        ));
        $this->assertEquals(
            [
                [
                    'ID' => 1,
                    'NAME' => 'Jan',
                ],
                [
                    'ID' => 2,
                    'NAME' => 'Josef',
                ],
            ],
            $result,
        );

        // check ALTER TABLE
        $db = $this->ensureProducerSnowflakeConnection();
        $db->executeQuery('USE ROLE ACCOUNTADMIN');
        $dbName = $this->getProducerSharedDatabase();
        $db->executeQuery(sprintf('USE WAREHOUSE %s', $this->getProducerSnowflakeWarehouse()));

        $db->executeQuery(sprintf(
            'ALTER TABLE %s.%s ADD COLUMN GENDER STRING DEFAULT \'M\'',
            $dbName,
            $newTableName,
        ));

        $this->_client->refreshBucket($bucketId);

        $refreshedBucket = $this->_client->getBucket($bucketId);
        $tableNames = array_map(
            function ($tableRow) {
                return $tableRow['name'];
            },
            $refreshedBucket['tables'],
        );
        $this->assertTrue(in_array($newTableName, $tableNames), 'Altered table not found in refreshed bucket');

        $filteredTables = array_filter(
            $refreshedBucket['tables'],
            function ($tableRow) use ($newTableName) {
                return $tableRow['name'] === $newTableName;
            },
        );

        $newTable = reset($filteredTables);

        $dataPreview = $this->_client->getTableDataPreview($newTable['id'], ['columns' => ['ID', 'NAME', 'GENDER']]);
        $this->assertEquals(
            <<<EXPECTED
"ID","NAME","GENDER"
"1","Jan","M"
"2","Josef","M"

EXPECTED,
            $dataPreview,
        );

        $linkingTables = $this->linkingClient->listTables($linkedBucketId);
        $this->assertCount(3, $linkingTables);

        $linkedBucket = $this->linkingClient->getBucket($linkedBucketId);
        $linkedTableNames = array_map(
            function ($tableRow) {
                return $tableRow['name'];
            },
            $linkedBucket['tables'],
        );
        $this->assertTrue(in_array($newTableName, $linkedTableNames), 'Altered table not found in linked bucket after refresh');

        $filteredTables = array_filter(
            $linkedBucket['tables'],
            function ($tableRow) use ($newTableName) {
                return $tableRow['name'] === $newTableName;
            },
        );

        $linkedTable = reset($filteredTables);

        $dataPreview = $this->linkingClient->getTableDataPreview($linkedTable['id'], ['columns' => ['ID', 'NAME', 'GENDER']]);
        $this->assertEquals(
            <<<EXPECTED
"ID","NAME","GENDER"
"1","Jan","M"
"2","Josef","M"

EXPECTED,
            $dataPreview,
        );

        // test RO works
        $result = $linkingSnowflakeDb->fetchAll(sprintf(
            'SELECT * FROM %s.%s',
            $linkedBucket['path'],
            $newTableName,
        ));
        $this->assertEquals(
            [
                [
                    'ID' => 1,
                    'NAME' => 'Jan',
                    'GENDER' => 'M',
                ],
                [
                    'ID' => 2,
                    'NAME' => 'Josef',
                    'GENDER' => 'M',
                ],
            ],
            $result,
        );

        // CLEANUP

        $this->forceUnshareBucketIfExists($this->shareClient, $stage . '.' . $bucketName, true);
        $this->_client->dropBucket($bucketId);
        $bucketExist = $this->_client->bucketExists($bucketId);
        $this->assertFalse($bucketExist, 'Bucket '.$bucketId.' still exist.');
        $this->dropTableInProducerDatabase($newTableName);
        $this->ensureSharedDatabaseStillExists();

        $workspaces->deleteWorkspace($workspace0['id']);
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
