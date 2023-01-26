<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\Test\Backend\Workspaces\Backend\TeradataWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

class TeradataRegisterBucketTest extends BaseExternalBuckets
{
    public function setUp(): void
    {
        parent::setUp();
        $token = $this->_client->verifyToken();

        $this->thisBackend = $token['owner']['defaultBackend'];

        if (!in_array('external-buckets', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
        }
        $this->allowTestForBackendsOnly([self::BACKEND_TERADATA], 'Backend has to support external buckets');
    }

    public function testInvalidDBToRegister(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration', true);

        try {
            $this->_client->registerBucket(
                'test-bucket-registration',
                ['non-existing-database'],
                'in',
                'will fail',
                $this->thisBackend,
                'test-bucket-will-fail'
            );
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
        $ws = new Workspaces($this->_client);

        $workspace = $ws->createWorkspace();

        // register bucket
        $this->initEvents($this->_client);
        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            $this->thisBackend,
            'Iam-your-workspace'
        );
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setRunId($runId)
            ->setObjectId($idOfBucket);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        //create table in the WS
        $this->initEvents($this->_client);
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'VARCHAR']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');

        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setRunId($runId)
            ->setObjectId('in.test-bucket-registration.TEST');
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketRefreshed')
            ->setRunId($runId)
            ->setObjectId($idOfBucket);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check metadata and data
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
            '38,19', // default length for TD
            $tableDetail['columnMetadata']['AMOUNT']
        );
        $this->assertColumnMetadata(
            'VARCHAR',
            '1',
            'STRING',
            '32000',
            $tableDetail['columnMetadata']['DESCRIPTION']
        );

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));

        // create talbe 2 in WS
        $this->initEvents($this->_client);
        $db->createTable('TEST2', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'VARCHAR']);
        $this->_client->refreshBucket($idOfBucket);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setRunId($runId)
            ->setObjectId('in.test-bucket-registration.TEST2');
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // drop table
        $db->dropTable('TEST2');
        $this->_client->refreshBucket($idOfBucket);

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableDeleted')
            ->setRunId($runId)
            ->setObjectId('in.test-bucket-registration.TEST2');
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // alter table
        $this->initEvents($this->_client);
        $db->executeQuery('ALTER TABLE "TEST" DROP "AMOUNT"');
        $db->executeQuery('ALTER TABLE "TEST" ADD "XXX" FLOAT');
        $this->_client->refreshBucket($idOfBucket);

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableColumnsUpdated')
                ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        // check columns after alter
        $tableDetail = $this->_client->getTable($tables[0]['id']);
        $this->assertSame(['DESCRIPTION', 'XXX'], $tableDetail['columns']);

        $this->assertColumnMetadata(
            'VARCHAR',
            '1',
            'STRING',
            '32000',
            $tableDetail['columnMetadata']['DESCRIPTION']
        );

        $this->assertColumnMetadata(
            'FLOAT',
            '1',
            'FLOAT',
            null,
            $tableDetail['columnMetadata']['XXX']
        );

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
    }

    public function testRegisterExternalDB(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration-ext', true);

        $this->initEvents($this->_client);
        $runId = $this->setRunId();
        // try same with schema outside of project database
        $token = $this->_client->verifyToken();
        $dbName = 'EXT_BUCKET_' . $token['owner']['id'];
        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration-ext',
            [$dbName],
            'in',
            'Iam in other database',
            'teradata',
            'Iam-from-external-db-ext'
        );
        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketCreated')
            ->setRunId($runId)
            ->setObjectId('in.test-bucket-registration-ext');
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tableCreated')
            ->setRunId($runId)
            ->setObjectId('in.test-bucket-registration-ext.TEST_TABLE');
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        // todo schema name should be asserted as soon as it appears in response

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);
        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));
        $this->_client->refreshBucket($idOfBucket);
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        // check that workspace user can read from table in external bucket directly

        $ws = new Workspaces($this->_client);

        $workspace = $ws->createWorkspace();
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        assert($db instanceof TeradataWorkspaceBackend);
        $result = $db->getDb()->fetchAllAssociative(
            sprintf(
                'SELECT COUNT(*) AS CNT FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($dbName),
                TeradataQuote::quoteSingleIdentifier('TEST_TABLE')
            )
        );
        $this->assertSame([
            [
                'CNT' => '1',
            ],
        ], $result);

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
    }
}
