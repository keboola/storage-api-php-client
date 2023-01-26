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
                'test-bucket-will-fail'
            );
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame('storage.dbObjectNotFound', $e->getStringCode());
            $this->assertStringContainsString(
                'doesn\'t exist or project user is missing privileges to read from it.',
                $e->getMessage()
            );
        }
    }

    public function testRegisterWSAsExternalBucket(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);
        $this->initEvents($this->_client);

        $ws = new Workspaces($this->_client);

        $workspace = $ws->createWorkspace();

        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'Iam-your-workspace'
        );
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');

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
            $tableDetail['columnMetadata']['AMOUNT']
        );
        $this->assertColumnMetadata(
            'VARCHAR',
            '1',
            'STRING',
            '16777216',
            $tableDetail['columnMetadata']['DESCRIPTION']
        );

        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));

        $db->createTable('TEST2', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

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

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $db->dropTable('TEST2');
        $db->executeQuery('ALTER TABLE "TEST" DROP COLUMN "AMOUNT"');
        $db->executeQuery('ALTER TABLE "TEST" ADD COLUMN "XXX" FLOAT');
        $db->createTable('TEST3', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

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

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $tableDetail = $this->_client->getTable($tables[0]['id']);
        $this->assertSame(['DESCRIPTION', 'XXX'], $tableDetail['columns']);

        $this->assertColumnMetadata(
            'VARCHAR',
            '1',
            'STRING',
            '16777216',
            $tableDetail['columnMetadata']['DESCRIPTION']
        );

        $this->assertColumnMetadata(
            'FLOAT',
            '1',
            'FLOAT',
            null,
            $tableDetail['columnMetadata']['XXX']
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
                ]
            );
        } catch (ClientException $e) {
            $this->assertSame('workspace.tableCannotBeLoaded', $e->getStringCode());
            $this->assertSame(
                'Table "test-bucket-registration" is part of external bucket "in.test-bucket-registration.TEST" and cannot be loaded into workspace.',
                $e->getMessage()
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
                ]
            );
        } catch (ClientException $e) {
            $this->assertSame('workspace.tableCannotBeLoaded', $e->getStringCode());
            $this->assertSame(
                'Table "test-bucket-registration" is part of external bucket "in.test-bucket-registration.TEST" and cannot be loaded into workspace.',
                $e->getMessage()
            );
        }

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
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
            'Iam-from-external-db-ext'
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

        $ws = new Workspaces($this->_client);

        $workspace = $ws->createWorkspace();
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // check that workspace user can read from table in external bucket directly
        assert($db instanceof SnowflakeWorkspaceBackend);
        $result = $db->getDb()->fetchAll(
            'SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA"."TEST_TABLE"'
        );
        $this->assertSame([
            [
                'CNT' => '1',
            ],
        ], $result);

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
    }
}
