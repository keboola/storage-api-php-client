<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

class SnowflakeRegisterBucketTest extends BaseExternalBuckets
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testRegisterBucket(): void
    {
        $this->initEvents($this->_client);
        $token = $this->_client->verifyToken();

        if (!in_array('input-mapping-read-only-storage', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Read only mapping is not enabled for project "%s"', $token['owner']['id']));
        }
        if (!in_array('external-buckets', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
        }
        $this->allowTestForBackendsOnly([self::BACKEND_SNOWFLAKE], 'Backend has to support external buckets');
        $this->expectNotToPerformAssertions();
    }

    public function testInvalidDBToRegister(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        try {
            $this->_client->registerBucket(
                'test-bucket-registration',
                ['non-existing-database', 'non-existing-schema'],
                'in',
                'will fail',
                'snowflake',
                'test-bucket-will-fail',
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.dbObjectNotFound', $e->getStringCode());
            $this->assertStringContainsString(
                'doesn\'t exist or project user is missing privileges to read from it.',
                $e->getMessage(),
            );
        }
    }

    public function testRegisterGuideShouldFailWithDifferentBackend(): void
    {
        try {
            $this->_client->registerBucketGuide(['test', 'test'], 'bigquery');
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('backendNotAllowed', $e->getStringCode());
            $this->assertStringContainsString('Backend "bigquery" is not assigned to the project.', $e->getMessage());
        }
    }

    public function testRegisterWSAsExternalBucket(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);
        $this->initEvents($this->_client);

        $ws = new Workspaces($this->_client);

        // prepare workspace
        $workspace = $ws->createWorkspace();

        $externalBucketPath = [$workspace['connection']['database'], $workspace['connection']['schema']];
        $externalBucketBackend = 'snowflake';
        $guide = $this->_client->registerBucketGuide($externalBucketPath, $externalBucketBackend);
        $this->assertArrayHasKey('markdown', $guide);
        $this->assertStringContainsString('GRANT USAGE ON DATABASE', $guide['markdown']);
        $this->assertStringContainsString('GRANT USAGE ON SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON ALL TABLES IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON FUTURE TABLES IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON ALL VIEWS IN SCHEMA', $guide['markdown']);
        $this->assertStringContainsString('GRANT SELECT ON FUTURE VIEWS IN SCHEMA', $guide['markdown']);

        // register workspace as external bucket
        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
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
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        // add first table to workspace
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $tableDetail = $this->_client->getTable($tables[0]['id']);

        $this->assertSame('KBC.dataTypesEnabled', $tableDetail['metadata'][0]['key']);
        $this->assertSame('true', $tableDetail['metadata'][0]['value']);
        $this->assertTrue($tableDetail['isTyped']);

        $this->assertCount(2, $tableDetail['columns']);

        $this->assertColumnMetadata(
            'NUMBER',
            '1',
            'NUMERIC',
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
        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));

        // add second table to workspace
        $db->createTable('TEST2', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        // alter first table, drop second table, add third table to workspace
        $db->dropTable('TEST2');
        $db->executeQuery('ALTER TABLE "TEST" DROP COLUMN "AMOUNT"');
        $db->executeQuery('ALTER TABLE "TEST" ADD COLUMN "XXX" FLOAT');
        $db->createTable('TEST3', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        // refresh external bucket
        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableDeleted')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableColumnsUpdated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $tableDetail = $this->_client->getTable($tables[0]['id']);
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

        // try failing load
        try {
            $ws->cloneIntoWorkspace(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tables[0]['id'],
                            'destination' => 'test',
                        ],
                    ],
                ],
            );
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('workspace.tableCannotBeLoaded', $e->getStringCode());
            $this->assertSame(
                'Table "test-bucket-registration" is part of external bucket "in.test-bucket-registration.TEST" and cannot be loaded into workspace.',
                $e->getMessage(),
            );
        }

        try {
            $ws->loadWorkspaceData(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tables[0]['id'],
                            'destination' => 'test',
                        ],
                    ],
                ],
            );
            $this->fail('Should fail');
        } catch (ClientException $e) {
            $this->assertSame('workspace.tableCannotBeLoaded', $e->getStringCode());
            $this->assertSame(
                'Table "test-bucket-registration" is part of external bucket "in.test-bucket-registration.TEST" and cannot be loaded into workspace.',
                $e->getMessage(),
            );
        }

        // drop external bucket
        $this->_client->dropBucket($idOfBucket, ['force' => true]);
    }

    public function testRegistrationOfExternalTable(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);
        $this->initEvents($this->_client);

        $ws = new Workspaces($this->_client);
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
        $idOfBucket = $this->_client->registerBucket(
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
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        // check table existence and metadata
        $tables = $this->_client->listTables($idOfBucket);
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
        $this->_client->refreshBucket($idOfBucket);

        // check table existence and metadata
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $firstTable = $tables[0];
        $this->assertEquals('MY_LITTLE_EXT_TABLE', $firstTable['name']);

        $this->assertSame($firstTable['tableType'], 'table');
    }

    public function testRegistrationOfView(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);
        $this->initEvents($this->_client);

        $ws = new Workspaces($this->_client);
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
        $idOfBucket = $this->_client->registerBucket(
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
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        // check table existence and metadata
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);
        $table = $tables[0];
        $this->assertEquals('MY_LITTLE_TABLE_FOR_VIEW', $table['name']);
        $this->assertEquals('table', $table['tableType']);
        $view = $tables[1];
        $this->assertEquals('MY_LITTLE_VIEW', $view['name']);
        $this->assertEquals('view', $view['tableType']);
    }

    public function testRegisterExternalDB(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration-ext', true);
        $this->initEvents($this->_client);
        $runId = $this->setRunId();
        // try same with schema outside of project database.
        // This DB has been created when test project was inited
        $idOfBucket = $this->_client->registerBucket(
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
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check external bucket
        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame('TEST_EXTERNAL_BUCKETS', $bucket['databaseName']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));
        $this->_client->refreshBucket($idOfBucket);
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        // check that workspace user CAN READ from table in external bucket directly
        $ws = new Workspaces($this->_client);

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
        $this->_client->dropBucket($idOfBucket, ['force' => true]);

        // check that workspace user CANNOT READ from table in external bucket directly
        try {
            $db->getDb()->fetchAll(
                'SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA"."TEST_TABLE"',
            );
            $this->fail('Database should not be authorized');
        } catch (\RuntimeException $e) {
            // produce WARNING
            $this->assertMatchesRegularExpression(
                "/Database 'TEST_EXTERNAL_BUCKETS' does not exist or not authorized/",
                $e->getMessage(),
            );
        }
    }

    public function testRefreshBucketWhenSchemaDoesNotExist(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);
        $this->initEvents($this->_client);

        $ws = new Workspaces($this->_client);
        // prepare workspace
        $workspace = $ws->createWorkspace();

        // register workspace as external bucket including external table
        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
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
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // delete workspace = simulates situation when BYODB owner simply deletes the registered schema -> it should also delete the bucket
        $ws->deleteWorkspace($workspace['id']);

        $this->_client->refreshBucket($idOfBucket);

        // bucket should be deleted
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Bucket in.test-bucket-registration not found');
        $this->_client->getBucket($idOfBucket);
    }


    public function testDropBucketWhenSchemaDoesNotExist(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);
        $this->initEvents($this->_client);

        $ws = new Workspaces($this->_client);
        // prepare workspace
        $workspace = $ws->createWorkspace();

        // register workspace as external bucket including external table
        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
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
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // delete workspace = simulates situation when BYODB owner simply deletes the registered schema -> should be able to delete the bucket
        $ws->deleteWorkspace($workspace['id']);

        $this->_client->dropBucket($idOfBucket, ['force' => true]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Bucket in.test-bucket-registration not found');
        $this->_client->getBucket($idOfBucket);
    }
}
