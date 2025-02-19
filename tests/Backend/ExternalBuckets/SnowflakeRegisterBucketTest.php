<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Db\Import\Snowflake\Connection as SnowflakeConnection;

class SnowflakeRegisterBucketTest extends BaseExternalBuckets
{
    protected \Keboola\StorageApi\Client $_testClient;

    public function setUp(): void
    {
        parent::setUp();
        [$devBranchType, $userRole] = $this->getProvidedData();
        $clientProvider = new ClientProvider($this);
        [$this->_client, $this->_testClient] = (new TestSetupHelper())->setUpForProtectedDevBranch(
            $clientProvider,
            $devBranchType,
            $userRole,
        );

        $this->initEmptyTestBucketsForParallelTests();

        $this->initEvents($this->_testClient);
        $token = $this->_testClient->verifyToken();

        if (!in_array('external-buckets', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
        }
        $this->allowTestForBackendsOnly([self::BACKEND_SNOWFLAKE], 'Backend has to support external buckets');
    }

    public function provideComponentsClientTypeBasedOnSuite(): array
    {
        return (new TestSetupHelper())->provideComponentsClientTypeBasedOnSuite($this);
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testInvalidDBToRegister(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);

        try {
            $this->_testClient->registerBucket(
                'test-bucket-registration',
                ['non-existing-database', 'non-existing-schema'],
                'in',
                'will fail',
                'snowflake',
                'test-bucket-will-fail',
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            if ($this->_testClient instanceof BranchAwareClient) {
                $this->assertSame('Cannot register bucket in dev branch', $e->getMessage());
            } else {
                $this->assertSame('storage.dbObjectNotFound', $e->getStringCode());
                $this->assertStringContainsString(
                    'doesn\'t exist or project user is missing privileges to read from it.',
                    $e->getMessage(),
                );
            }
        }
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterGuideShouldFailWithDifferentBackend(): void
    {
        try {
            $this->_testClient->registerBucketGuide(['test', 'test'], 'bigquery');
            $this->fail('should fail');
        } catch (ClientException $e) {
            if ($this->_testClient instanceof BranchAwareClient) {
                $this->assertSame('This endpoint is available in the default branch only.', $e->getMessage());
            } else {
                $this->assertSame('storage.backendNotAllowed', $e->getStringCode());
                $this->assertStringContainsString('Backend "bigquery" is not assigned to the project.', $e->getMessage());
            }
        }
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterTableWithWrongName(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.bucket-registration-wrong-table-name', true);

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();
        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';

        // add first table to workspace with long name, table should be skipped
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $longTableName = str_repeat('TableTestLong', 8); // 104 chars
        $db->createTable($longTableName, ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        $db->createTable(
            'moje nova 1234 -2ěščěš',
            [
                'AMOUNT' => 'INT',
                'DESCRIPTION' => 'STRING',
            ],
        );
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // Api endpoint return warning, but client method return only bucket id
        // I added warning message to logs
        $idOfBucket = $this->_testClient->registerBucket(
            'bucket-registration-wrong-table-name',
            $externalBucketPath,
            'in',
            'Iam in workspace',
            $externalBucketBackend,
            'Bucket-with-wrong-table-name',
        );

        // only table with long name is there and is skipped
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        $refreshJobResult = $this->_testClient->refreshBucket($idOfBucket);
        assert(is_array($refreshJobResult));
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        $this->assertCount(2, $refreshJobResult['warnings']);
        $this->assertSame(
            '\'TableTestLongTableTestLongTableTestLongTableTestLongTableTestLongTableTestLongTableTestLongTableTestLong\' is more than 96 characters long',
            $refreshJobResult['warnings'][0]['message'],
        );
        $this->assertSame(
            '\'moje nova 1234 -2ěščěš\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
            $refreshJobResult['warnings'][1]['message'],
        );

        $db->createTable('normalTable', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        // new table should be added, and warning for table with long name should be returned
        $refreshJobResult = $this->_testClient->refreshBucket($idOfBucket);
        assert(is_array($refreshJobResult));
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        $this->assertCount(2, $refreshJobResult['warnings']);
        $this->assertSame(
            '\'TableTestLongTableTestLongTableTestLongTableTestLongTableTestLongTableTestLongTableTestLongTableTestLong\' is more than 96 characters long',
            $refreshJobResult['warnings'][0]['message'],
        );
        $this->assertSame(
            '\'moje nova 1234 -2ěščěš\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
            $refreshJobResult['warnings'][1]['message'],
        );
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterTablesWithDuplicateNameWithDifferentCase(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.bucket-registration-duplicate-table-name', true);

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();
        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';

        // add first table to workspace with long name, table should be skipped
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable(
            'test1',
            [
                'AMOUNT' => 'INT',
            ],
        );
        $db->createTable(
            'test2',
            [
                'AMOUNT' => 'INT',
            ],
        );
        $db->createTable(
            'tEst1',
            [
                'AMOUNT' => 'INT',
            ],
        );

        try {
            $this->_testClient->registerBucket(
                'bucket-registration-duplicate-table-name',
                $externalBucketPath,
                'in',
                'Iam in workspace',
                $externalBucketBackend,
                'Bucket-with-duplicate-table-name',
            );
            $this->fail('Register bucket should fail with duplicate table name');
        } catch (ClientException $e) {
            if ($this->_testClient instanceof BranchAwareClient) {
                $this->assertSame('Cannot register bucket in dev branch', $e->getMessage());
            } else {
                $this->assertSame('storage.duplicateTableNamesInSchema', $e->getStringCode());
                $this->assertSame(
                    'Multiple tables with the same name detected. Table names are case-insensitive, leading to duplicates: "tEst1, test1"',
                    $e->getMessage(),
                );
            }
        }
        // refresh
        $db->dropTable('tEst1');
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        $registeredBucketId = $this->_testClient->registerBucket(
            'bucket-registration-duplicate-table-name',
            $externalBucketPath,
            'in',
            'Iam in workspace',
            $externalBucketBackend,
            'Bucket-with-duplicate-table-name',
        );
        $db->createTable(
            'tEst1',
            [
                'AMOUNT' => 'INT',
            ],
        );
        try {
            $this->_testClient->refreshBucket($registeredBucketId);
            $this->fail('Refresh bucket should fail with duplicate table name');
        } catch (ClientException $e) {
            if ($this->_testClient instanceof BranchAwareClient) {
                $this->assertSame('This endpoint is available in the default branch only.', $e->getMessage());
            } else {
                $this->assertSame('storage.duplicateTableNamesInSchema', $e->getStringCode());
                $this->assertSame(
                    'Multiple tables with the same name detected. Table names are case-insensitive, leading to duplicates: "tEst1, test1"',
                    $e->getMessage(),
                );
            }
        }
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterWSAsExternalBucket(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);
        $this->initEvents($this->_testClient);

        $ws = new Workspaces($this->_testClient);

        // prepare workspace
        $workspace = $ws->createWorkspace();

        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('This endpoint is available in the default branch only.');
        }
        $guide = $this->_testClient->registerBucketGuide($externalBucketPath, $externalBucketBackend);
        $this->assertArrayHasKey('markdown', $guide);
        $this->assertStringContainsString('GRANT USAGE ON DATABASE', $guide['markdown']);
        $this->assertStringContainsString('GRANT USAGE ON SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON ALL TABLES IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON FUTURE TABLES IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON ALL VIEWS IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON FUTURE VIEWS IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA', $guide['markdown']);

        // register workspace as external bucket
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-registration',
            $externalBucketPath,
            'in',
            'Iam in workspace',
            $externalBucketBackend,
            'Iam-your-workspace',
        );
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_testClient->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        // add first table to workspace
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        $this->_testClient->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // check external bucket
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $tableDetail = $this->_testClient->getTable($tables[0]['id']);

        $this->assertSame('KBC.dataTypesEnabled', $tableDetail['metadata'][0]['key']);
        $this->assertSame('true', $tableDetail['metadata'][0]['value']);
        $this->assertTrue($tableDetail['isTyped']);

        $this->assertCount(2, $tableDetail['columns']);

        $this->assertColumnMetadata(
            'NUMBER',
            '1',
            'INTEGER',
            '38,0',
            $tableDetail['columnMetadata']['AMOUNT'],
        );
        $this->assertColumnMetadata(
            'VARCHAR',
            '1',
            'STRING',
            '16777216',
            $tableDetail['columnMetadata']['DESCRIPTION'],
        );

        // export table from external bucket
        $this->_testClient->exportTableAsync($tables[0]['id']);

        $preview = $this->_testClient->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));

        // add second table to workspace
        $db->createTable('TEST2', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        $this->_testClient->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // check external bucket
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        // alter first table, drop second table, add third table to workspace
        $db->dropTable('TEST2');
        $db->executeQuery('ALTER TABLE "TEST" DROP COLUMN "AMOUNT"');
        $db->executeQuery('ALTER TABLE "TEST" ADD COLUMN "XXX" FLOAT');
        $db->createTable('TEST3', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        $this->_testClient->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableDeleted')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableColumnsUpdated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // check external bucket
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $tableDetail = $this->_testClient->getTable($tables[0]['id']);
        $this->assertSame(['DESCRIPTION', 'XXX'], $tableDetail['columns']);

        $this->assertColumnMetadata(
            'VARCHAR',
            '1',
            'STRING',
            '16777216',
            $tableDetail['columnMetadata']['DESCRIPTION'],
        );

        $this->assertColumnMetadata(
            'FLOAT',
            '1',
            'FLOAT',
            null,
            $tableDetail['columnMetadata']['XXX'],
        );

        $destinationWorkspace = $ws->createWorkspace();
        $destinationDb = WorkspaceBackendFactory::createWorkspaceBackend($destinationWorkspace, true);

        // load from external bucket
        $destinationDb->createTable('COPY_TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        try {
            // clone from external bucket fail
            $ws->cloneIntoWorkspace(
                $destinationWorkspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tables[0]['id'],
                            'destination' => 'COPY_TEST',
                        ],
                    ],
                ],
            );
            $this->fail('Should have failed clone is not supported for external buckets');
        } catch (ClientException $e) {
            $this->assertSame('workspace.loadRequestBadInput', $e->getStringCode());
            $this->assertSame(400, $e->getCode());
        }

        $ws->loadWorkspaceData(
            $destinationWorkspace['id'],
            [
                'input' => [
                    [
                        'source' => $tables[0]['id'],
                        'destination' => 'COPY_TEST',
                    ],
                    [
                        'source' => $tables[0]['id'],
                        'destination' => 'COPY_TEST_VIEW',
                        'useView' => true,
                    ],
                ],
            ],
        );

        $schema = $destinationDb->getSchemaReflection();
        $this->assertSame(['COPY_TEST'], $schema->getTablesNames());
        $this->assertSame(['COPY_TEST_VIEW'], $schema->getViewsNames());

        $destinationTableData = $destinationDb->fetchAll('COPY_TEST');
        $this->assertCount(2, $destinationTableData[0]);
        $this->assertEquals(['test', null], $destinationTableData[0]);

        $destinationTableData = $destinationDb->fetchAll('COPY_TEST_VIEW');
        $this->assertCount(2, $destinationTableData[0]);
        $this->assertEquals(['test', null], $destinationTableData[0]);

        // drop destination workspace
        $ws->deleteWorkspace($destinationWorkspace['id']);
        // drop external bucket
        $this->_testClient->dropBucket($idOfBucket, ['force' => true]);
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testAliasFromExternalBucketNotAllowed(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);
        $this->initEvents($this->_testClient);

        $ws = new Workspaces($this->_testClient);

        // prepare workspace
        $workspace = $ws->createWorkspace([], true);
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');

        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // register workspace as external bucket
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-registration',
            $externalBucketPath,
            'in',
            'Iam in workspace',
            $externalBucketBackend,
            'Iam-your-workspace',
        );

        // test that it's not possible to alias table in external bucket
        $testBucketId = $this->getTestBucketId();
        $testTableId = $this->_testClient->getTableId('TEST', $idOfBucket);
        assert(is_string($testTableId));
        try {
            $this->_testClient->createAliasTable(
                $testBucketId,
                $testTableId,
                'testAlias',
            );

            // delete bucket in case the call didn't fail
            $this->_testClient->dropBucket($idOfBucket, ['force' => true]);

            $this->fail('Should have failed');
        } catch (ClientException $e) {
            $this->assertSame(
                'Creating an alias from a table in an external bucket is not supported.',
                $e->getMessage(),
            );
            $this->assertSame(
                'storage.buckets.invalidAlias',
                $e->getStringCode(),
            );
        }
        $this->_testClient->dropBucket($idOfBucket, ['force' => true]);
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegistrationOfExternalTable(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);
        $this->initEvents($this->_testClient);

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // doesn't matter that the data are not valid, we just need to create the table structure
        $db->executeQuery(
            <<<SQL
CREATE OR REPLACE STAGE s3_stage URL = 's3://xxxx'
    CREDENTIALS = ( AWS_KEY_ID = 'XXX' AWS_SECRET_KEY = 'YYY');
SQL,
        );
        $db->executeQuery(
            <<<SQL
CREATE OR REPLACE
EXTERNAL TABLE MY_LITTLE_EXT_TABLE (
    ID NUMBER(38,0) AS (VALUE:c1::INT),
    FIRST_NAME VARCHAR(255) AS (VALUE:c2::STRING)
    ) 
    LOCATION=@s3_stage/data 
    REFRESH_ON_CREATE = FALSE 
    AUTO_REFRESH = FALSE 
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 TRIM_SPACE=TRUE );
SQL,
        );

        // register workspace as external bucket including external table
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'Iam-your-workspace',
        );
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_testClient->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        // check table existence and metadata
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $firstTable = $tables[0];
        $this->assertEquals('MY_LITTLE_EXT_TABLE', $firstTable['name']);

        $this->assertSame($firstTable['tableType'], 'snowflake-external-table');

        $db->executeQuery(
            <<<SQL
DROP TABLE MY_LITTLE_EXT_TABLE;
SQL,
        );
        $db->createTable('MY_LITTLE_EXT_TABLE', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        $this->_testClient->refreshBucket($idOfBucket);

        // check table existence and metadata
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $firstTable = $tables[0];
        $this->assertEquals('MY_LITTLE_EXT_TABLE', $firstTable['name']);

        $this->assertSame($firstTable['tableType'], 'table');
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegistrationOfView(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);
        $this->initEvents($this->_testClient);

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('MY_LITTLE_TABLE_FOR_VIEW', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        // doesn't matter that the data are not valid, we just need to create the table structure
        $db->executeQuery(
            <<<SQL
CREATE OR REPLACE VIEW MY_LITTLE_VIEW AS SELECT * FROM  MY_LITTLE_TABLE_FOR_VIEW;
SQL,
        );

        // register workspace as external bucket including external table
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'Iam-your-workspace',
        );
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_testClient->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        // check table existence and metadata
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(2, $tables);
        $table = $tables[0];
        $this->assertEquals('MY_LITTLE_TABLE_FOR_VIEW', $table['name']);
        $this->assertEquals('table', $table['tableType']);
        $view = $tables[1];
        $this->assertEquals('MY_LITTLE_VIEW', $view['name']);
        $this->assertEquals('view', $view['tableType']);

        // test load
        $destinationWorkspace = $ws->createWorkspace();
        $destinationDb = WorkspaceBackendFactory::createWorkspaceBackend($destinationWorkspace, true);
        try {
            // clone from external bucket fail
            $ws->cloneIntoWorkspace(
                $destinationWorkspace['id'],
                [
                    'input' => [
                        [
                            'source' => $view['id'],
                            'destination' => 'COPY_TEST',
                        ],
                    ],
                ],
            );
            $this->fail('Should have failed clone is not supported for external buckets');
        } catch (ClientException $e) {
            $this->assertSame('workspace.loadRequestBadInput', $e->getStringCode());
            $this->assertSame(400, $e->getCode());
        }

        $ws->loadWorkspaceData(
            $destinationWorkspace['id'],
            [
                'input' => [
                    [
                        'source' => $view['id'],
                        'destination' => 'COPY_TEST',
                    ],
                    [
                        'source' => $view['id'],
                        'destination' => 'COPY_TEST_VIEW',
                        'useView' => true,
                    ],
                ],
            ],
        );

        $schema = $destinationDb->getSchemaReflection();
        $this->assertSame(['COPY_TEST'], $schema->getTablesNames());
        $this->assertSame(['COPY_TEST_VIEW'], $schema->getViewsNames());
        // drop destination workspace
        $ws->deleteWorkspace($destinationWorkspace['id']);
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testCreateSnapshotFromExternalBucketIsNotSupported(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_testClient, $bucketId, true);

        $ws = new Workspaces($this->_testClient);
        $workspace = $ws->createWorkspace();

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TABLE_FOR_SNAPSHOT', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        $idOfBucket = $this->_testClient->registerBucket(
            $testBucketName,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'You-cant-create-snapshot-from-me',
        );

        $tables = $this->_testClient->listTables($idOfBucket);

        try {
            $this->_testClient->createTableSnapshot(reset($tables)['id']);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('storage.buckets.snapshotNotSupported', $e->getStringCode());
            $this->assertSame('Creating snapshots from tables in external buckets is not supported.', $e->getMessage());
        }
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testAlteredColumnThrowsUserExAndAfterRefreshWillWork(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $testBucketName = $this->getTestBucketName($description);
        $bucketId = self::STAGE_IN . '.' . $testBucketName;

        $this->dropBucketIfExists($this->_testClient, $bucketId, true);

        $ws = new Workspaces($this->_testClient);
        $workspace = $ws->createWorkspace();

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('MY_LITTLE_TABLE_FOR_VIEW', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        $db->executeQuery(
            <<<SQL
CREATE OR REPLACE VIEW MY_LITTLE_VIEW AS SELECT * FROM  MY_LITTLE_TABLE_FOR_VIEW;
SQL,
        );
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        $idOfBucket = $this->_testClient->registerBucket(
            $testBucketName,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'Iam-your-external-bucket-to-test-alter-table',
        );

        $bucket = $this->_testClient->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        // check table existence
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(2, $tables);
        $table = $tables[0];
        $view = $tables[1];

        // now add new column to table and recreate view, to test preview still works
        $db->executeQuery(
            <<<SQL
ALTER TABLE MY_LITTLE_TABLE_FOR_VIEW ADD COLUMN AGE INTEGER;
SQL,
        );

        $db->executeQuery(
            <<<SQL
CREATE OR REPLACE VIEW MY_LITTLE_VIEW AS SELECT * FROM  MY_LITTLE_TABLE_FOR_VIEW;
SQL,
        );

        /** @var array $tableDataPreview */
        $tableDataPreview = $this->_testClient->getTableDataPreview($table['id'], ['format' => 'json']);
        /** @var array $viewDataPreview1 */
        $viewDataPreview1 = $this->_testClient->getTableDataPreview($view['id'], ['format' => 'json']);
        $this->assertSame(['AMOUNT', 'DESCRIPTION'], $tableDataPreview['columns']);
        $this->assertSame(['AMOUNT', 'DESCRIPTION'], $viewDataPreview1['columns']);

        $expectationsFileWithTwoCols = __DIR__ . '/../../_data/export/with-two-columns.csv';
        $expectationsFileWithThreeCols = __DIR__ . '/../../_data/export/with-three-columns.csv';
        $exporter = new TableExporter($this->_testClient);

        $exporter->exportTable($table['id'], $this->getExportFilePathForTest('with-two-cols.csv'), []);
        $this->assertLinesEqualsSorted(
            file_get_contents($expectationsFileWithTwoCols),
            file_get_contents($this->getExportFilePathForTest('with-two-cols.csv')),
        );

        $exporter->exportTable($view['id'], $this->getExportFilePathForTest('with-two-cols.csv'), []);
        $this->assertLinesEqualsSorted(
            file_get_contents($expectationsFileWithTwoCols),
            file_get_contents($this->getExportFilePathForTest('with-two-cols.csv')),
        );

        $this->_testClient->refreshBucket($idOfBucket);

        // test after refresh, still works
        /** @var array $tableDataPreview */
        $tableDataPreview = $this->_testClient->getTableDataPreview($table['id'], ['format' => 'json']);
        /** @var array $viewDataPreview1 */
        $viewDataPreview1 = $this->_testClient->getTableDataPreview($view['id'], ['format' => 'json']);
        $this->assertSame(['AMOUNT', 'DESCRIPTION', 'AGE'], $tableDataPreview['columns']);
        $this->assertSame(['AMOUNT', 'DESCRIPTION', 'AGE'], $viewDataPreview1['columns']);

        $exporter->exportTable($table['id'], $this->getExportFilePathForTest('with-three-cols.csv'), []);
        $this->assertLinesEqualsSorted(
            file_get_contents($expectationsFileWithThreeCols),
            file_get_contents($this->getExportFilePathForTest('with-three-cols.csv')),
        );

        $exporter->exportTable($view['id'], $this->getExportFilePathForTest('with-three-cols.csv'), []);
        $this->assertLinesEqualsSorted(
            file_get_contents($expectationsFileWithThreeCols),
            file_get_contents($this->getExportFilePathForTest('with-three-cols.csv')),
        );

        // drop column and recreate view, to test preview ends with user err
        $db->executeQuery(
            <<<SQL
ALTER TABLE MY_LITTLE_TABLE_FOR_VIEW DROP COLUMN AGE;
SQL,
        );

        $db->executeQuery(
            <<<SQL
CREATE OR REPLACE VIEW MY_LITTLE_VIEW AS SELECT * FROM  MY_LITTLE_TABLE_FOR_VIEW;
SQL,
        );

        try {
            $this->_testClient->getTableDataPreview($table['id'], ['format' => 'json']);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('storage.backend.externalBucketObjectInvalidIdentifier', $e->getStringCode());
            $this->assertStringContainsString('External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.', $e->getMessage());
        }

        try {
            $this->_testClient->getTableDataPreview($view['id'], ['format' => 'json']);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('storage.backend.externalBucketObjectInvalidIdentifier', $e->getStringCode());
            $this->assertStringContainsString('External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.', $e->getMessage());
        }

        try {
            $exporter->exportTable($table['id'], $this->getExportFilePathForTest('with-three-cols.csv'), []);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.backend.externalBucketObjectInvalidIdentifier', $e->getStringCode());
            $this->assertStringContainsString('External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.', $e->getMessage());
        }

        try {
            $exporter->exportTable($table['id'], $this->getExportFilePathForTest('with-three-cols.csv'), []);
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.backend.externalBucketObjectInvalidIdentifier', $e->getStringCode());
            $this->assertStringContainsString('External object might be out of sync. Please refresh the external bucket to ensure it\'s synchronized, then try again.', $e->getMessage());
        }

        // after refresh should work again
        $this->_testClient->refreshBucket($idOfBucket);

        /** @var array $tableDataPreview */
        $tableDataPreview = $this->_testClient->getTableDataPreview($table['id'], ['format' => 'json']);
        /** @var array $viewDataPreview1 */
        $viewDataPreview1 = $this->_testClient->getTableDataPreview($view['id'], ['format' => 'json']);
        $this->assertSame(['AMOUNT', 'DESCRIPTION'], $tableDataPreview['columns']);
        $this->assertSame(['AMOUNT', 'DESCRIPTION'], $viewDataPreview1['columns']);

        $exporter->exportTable($table['id'], $this->getExportFilePathForTest('with-two-cols.csv'), []);
        $this->assertLinesEqualsSorted(
            file_get_contents($expectationsFileWithTwoCols),
            file_get_contents($this->getExportFilePathForTest('with-two-cols.csv')),
        );

        $exporter->exportTable($view['id'], $this->getExportFilePathForTest('with-two-cols.csv'), []);
        $this->assertLinesEqualsSorted(
            file_get_contents($expectationsFileWithTwoCols),
            file_get_contents($this->getExportFilePathForTest('with-two-cols.csv')),
        );
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterExternalDB(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration-ext', true);
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration-ext2', true);
        $this->initEvents($this->_testClient);
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // try same with schema outside of project database.
        // This DB has been created when test project was inited
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-registration-ext',
            ['TEST_EXTERNAL_BUCKETS', 'TEST_SCHEMA'],
            'in',
            'Iam in other database',
            'snowflake',
            'Iam-from-external-db-ext',
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(3, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $idOfBucket2 = $this->_testClient->registerBucket(
            'test-bucket-registration-ext2',
            ['TEST_EXTERNAL_BUCKETS', 'TEST_SCHEMA2'],
            'in',
            'Iam in other database',
            'snowflake',
            'Iam-from-external-db-ext2',
        );

        // check external bucket
        $bucket = $this->_testClient->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame('TEST_EXTERNAL_BUCKETS', $bucket['databaseName']);

        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(3, $tables);
        $tableResponse = $viewResponse = $externalTable = null;

        foreach ($tables as $table) {
            switch ($table['name']) {
                case 'TEST_TABLE':
                    $tableResponse = $table;
                    break;
                case 'TEST_VIEW':
                    $viewResponse = $table;
                    break;
                case 'TEST_EXTERNAL_TABLE':
                    $externalTable = $table;
                    break;
                default:
                    throw new \RuntimeException('Unexpected object in external bucket');
            }
        }
        $this->_testClient->exportTableAsync($tableResponse['id']);
        $this->assertEquals('snowflake-external-table', $externalTable['tableType']);
        $this->assertEquals('view', $viewResponse['tableType']);
        $previewTable = $this->_testClient->getTableDataPreview($tableResponse['id']);
        $previewView = $this->_testClient->getTableDataPreview($viewResponse['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($previewTable, false));
        $this->assertCount(2, Client::parseCsv($previewView, false));
        $this->_testClient->refreshBucket($idOfBucket);
        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(3, $tables);

        // check that workspace user CAN READ from table in external bucket directly
        $ws = new Workspaces($this->_testClient);

        $workspace = $ws->createWorkspace();
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        assert($db instanceof SnowflakeWorkspaceBackend);
        $result = $db->getDb()->fetchAll(
            'SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA"."TEST_TABLE"',
        );
        $this->assertSame([
            [
                'CNT' => '1',
            ],
        ], $result);

        // drop external bucket
        $this->_testClient->dropBucket($idOfBucket, ['force' => true]);

        // check that workspace user CANNOT READ from table in external bucket directly
        try {
            $db->getDb()->fetchAll(
                'SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA"."TEST_TABLE"',
            );
            $this->fail('Schema should not be authorized anymore');
        } catch (\RuntimeException $e) {
            // produce WARNING. The error is on Schema level -> so REVOKE was performed on Schema level
            $this->assertMatchesRegularExpression(
                "/Schema 'TEST_EXTERNAL_BUCKETS.TEST_SCHEMA' does not exist or not authorized/",
                $e->getMessage(),
            );
        }
        // I can still access other external buckets
        $rowsCount = $db->getDb()->fetchAll(
            'SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA2"."TEST_TABLE2"',
        );
        $this->assertSame((int) $rowsCount, 1);

        $this->_testClient->dropBucket($idOfBucket2, ['force' => true]);

        try {
            $db->getDb()->fetchAll(
                'SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA2"."TEST_TABLE2"',
            );
            $this->fail('Database should not be authorized');
        } catch (\RuntimeException $e) {
            // produce WARNING. The error is on DB level -> so REVOKE on DB level has been performed
            $this->assertMatchesRegularExpression(
                "/Database 'TEST_EXTERNAL_BUCKETS' does not exist or not authorized/",
                $e->getMessage(),
            );
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterExternalDBWithNoWS(): void
    {
        $wsService = new Workspaces($this->_testClient);
        $allWorkspacesInThisProject = $wsService->listWorkspaces();
        foreach ($allWorkspacesInThisProject as $workspace) {
            $wsService->deleteWorkspace($workspace['id']);
        }

        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-reg-ext-no-ws', true);
        $this->initEvents($this->_testClient);
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // try same with schema outside of project database.
        // This DB has been created when test project was inited
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-reg-ext-no-ws',
            ['TEST_EXTERNAL_BUCKETS', 'TEST_SCHEMA'],
            'in',
            'Iam in other database',
            'snowflake',
            'Iam-from-external-db-ext-no-ws',
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(3, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // it should be easily dropped even no WS exists
        $this->_testClient->dropBucket($idOfBucket, ['force' => true]);
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRefreshBucketWhenSchemaDoesNotExist(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);
        $this->initEvents($this->_testClient);

        $token = $this->_client->verifyToken();
        $projectId = $token['owner']['id'];

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // register workspace as external bucket including external table
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        $idOfBucket = $this->_testClient->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'Iam-your-workspace',
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // delete workspace = simulates situation when BYODB owner simply deletes the registered schema -> it should also delete the bucket
        $ws->deleteWorkspace($workspace['id']);

        // bucket shouldn't be deleted and exception should be thrown
        try {
            $this->_testClient->refreshBucket($idOfBucket);
            $this->fail('should fail');
        } catch (ClientException $e) {
            if ($this->_testClient instanceof BranchAwareClient) {
                $this->assertSame('This endpoint is available in the default branch only.', $e->getMessage());
            } else {
                $this->assertStringContainsString('doesn\'t exist or project user is missing privileges to read from it.', $e->getMessage());
            }
        }

        // test bucket still exists
        $bucket = $this->_testClient->getBucket($idOfBucket);
        $this->assertNotEmpty($bucket);

        $this->_testClient->dropBucket($idOfBucket);

        // test bucket is deleted
        try {
            $this->_testClient->getBucket($idOfBucket);
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame(sprintf('The bucket "%s" was not found in the project "%s"', $idOfBucket, $projectId), $e->getMessage());
        }
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testDropBucketWhenSchemaDoesNotExist(): void
    {
        $bucketStage = self::STAGE_IN;
        $bucketName = 'test-bucket-registration';
        $bucketId = 'in.test-bucket-registration';

        $this->dropBucketIfExists($this->_testClient, $bucketId, true);
        $this->initEvents($this->_testClient);

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // register workspace as external bucket including external table
        $runId = $this->setRunId();
        $this->_testClient->setRunId($runId);
        $idOfBucket = $this->_testClient->registerBucket(
            $bucketName,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            $bucketStage,
            'Iam in workspace',
            'snowflake',
            'Iam-your-workspace',
        );

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        // delete workspace = simulates situation when BYODB owner simply deletes the registered schema -> should be able to delete the bucket
        $ws->deleteWorkspace($workspace['id']);

        $this->_testClient->dropBucket($idOfBucket, ['force' => true]);

        $token = $this->_testClient->verifyToken();
        $projectId = $token['owner']['id'];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('The bucket "%s" was not found in the project "%s"', $bucketId, $projectId));
        $this->_testClient->getBucket($idOfBucket);
    }
    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testDropExternalBucket(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.external_bucket_1', true);
        $this->dropBucketIfExists($this->_testClient, 'in.external_bucket_2', true);

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();
        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];

        // add first table to workspace with long name, table should be skipped
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable(
            'test1',
            [
                'AMOUNT' => 'INT',
            ],
        );
        $db->executeQuery('INSERT INTO "test1" VALUES (0)');

        // prepare workspace
        $workspace2 = $ws->createWorkspace();
        $externalBucketPath2 = [$workspace2['connection']['database'], $workspace2['connection']['schema']];

        // add first table to workspace with long name, table should be skipped
        $db2 = WorkspaceBackendFactory::createWorkspaceBackend($workspace2);

        $db2->createTable(
            'test2',
            [
                'AMOUNT' => 'INT',
            ],
        );
        $db2->executeQuery('INSERT INTO "test2" VALUES (1)');
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        $bucket1ID = $this->_testClient->registerBucket(
            'external_bucket_1',
            $externalBucketPath,
            'in',
            'Iam in workspace',
            'snowflake',
            'external_bucket_1',
        );

        $this->_testClient->registerBucket(
            'external_bucket_2',
            $externalBucketPath2,
            'in',
            'Iam in workspace',
            'snowflake',
            'external_bucket_2',
        );

        // workspace which will check data using RO-IM
        $workspaceForChecking = $ws->createWorkspace();
        /** @var SnowflakeConnection $workspaceDbForChecking */
        $workspaceDbForChecking = WorkspaceBackendFactory::createWorkspaceBackend($workspaceForChecking)->getDb();

        // listing data via RO-IM from both workspaces
        $dataFromBucket1 = $workspaceDbForChecking->fetchAll(
            sprintf(
                'SELECT * FROM %s.%s.%s',
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['database']),
                SnowflakeQuote::quoteSingleIdentifier($workspace['connection']['schema']),
                SnowflakeQuote::quoteSingleIdentifier('test1'),
            ),
        );
        $dataFromBucket2BeforeDeletion = $workspaceDbForChecking->fetchAll(
            sprintf(
                'SELECT * FROM %s.%s.%s',
                SnowflakeQuote::quoteSingleIdentifier($workspace2['connection']['database']),
                SnowflakeQuote::quoteSingleIdentifier($workspace2['connection']['schema']),
                SnowflakeQuote::quoteSingleIdentifier('test2'),
            ),
        );

        // asserting expected data
        $this->assertEquals(['AMOUNT' => '0'], $dataFromBucket1[0]);
        $this->assertEquals(['AMOUNT' => '1'], $dataFromBucket2BeforeDeletion[0]);

        // dropping the bucket
        $this->_testClient->dropBucket($bucket1ID);

        // RO role should keep access to existing external bucket (=workspace)
        $dataFrom2AfterDeletion = $workspaceDbForChecking->fetchAll(
            sprintf(
                'SELECT * FROM %s.%s.%s',
                SnowflakeQuote::quoteSingleIdentifier($workspace2['connection']['database']),
                SnowflakeQuote::quoteSingleIdentifier($workspace2['connection']['schema']),
                SnowflakeQuote::quoteSingleIdentifier('test2'),
            ),
        );
        $this->assertEquals($dataFromBucket2BeforeDeletion, $dataFrom2AfterDeletion);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterSharedDatabaseExternalBucket(): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.test-bucket-registration', true);
        $this->initEvents($this->_testClient);

        $ws = new Workspaces($this->_testClient);

        // prepare workspace
        $workspace = $ws->createWorkspace();

        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('This endpoint is available in the default branch only.');
        }
        $guide = $this->_testClient->registerBucketGuide(
            path: $externalBucketPath,
            backend: $externalBucketBackend,
            isSnowflakeSharedDatabase: true,
        );
        $this->assertArrayHasKey('markdown', $guide);
        $this->assertStringContainsString('GRANT IMPORTED PRIVILEGES ON DATABASE', $guide['markdown']);
    }


    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testRegisterTableAndChangeColumnCase(): void
    {
        $name = $this->getTestBucketName($this->generateDescriptionForTestObject());
        $bucketName = 'in.' . $name;
        $this->dropBucketIfExists(
            $this->_testClient,
            $bucketName,
            true,
        );

        $ws = new Workspaces($this->_testClient);
        // prepare workspace
        $workspace = $ws->createWorkspace();
        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';

        // add first table to workspace with long name, table should be skipped
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable(
            'test',
            [
                'amount' => 'INT',
                'description' => 'STRING',
            ],
        );
        if ($this->_testClient instanceof BranchAwareClient) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage('Cannot register bucket in dev branch');
        }
        // Api endpoint return warning, but client method return only bucket id
        // I added warning message to logs
        $idOfBucket = $this->_testClient->registerBucket(
            $name,
            $externalBucketPath,
            'in',
            'ItestRegisterTableAndChangeColumnCase',
            $externalBucketBackend,
            'testRegisterTableAndChangeColumnCase',
        );

        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        $db->dropTable('test');
        $db->createTable(
            'test',
            [
                'AMOUNT' => 'INT',
                'DESCRIPTION' => 'STRING',
            ],
        );

        $this->_testClient->refreshBucket($idOfBucket);

        $tables = $this->_testClient->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        $table = $this->_testClient->getTable($tables[0]['id']);
        $this->assertCount(2, $table['columns']);
        $this->assertCount(2, $table['definition']['columns']);
    }
}
