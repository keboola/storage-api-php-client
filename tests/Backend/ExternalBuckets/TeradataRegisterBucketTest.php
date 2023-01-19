<?php

namespace Keboola\Test\Backend\ExternalBuckets;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\Test\Backend\Workspaces\Backend\TeradataWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

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
        $ws = new Workspaces($this->_client);

        $workspace = $ws->createWorkspace();

        $runId = $this->setRunId();
        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            $this->thisBackend,
            'Iam-your-workspace'
        );
        $this->assertEvents($runId, ['storage.bucketCreated']);

        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'VARCHAR']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');

        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);
        $this->assertEvents($runId, ['storage.tableCreated', 'storage.bucketRefreshed']);

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

        $db->createTable('TEST2', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'VARCHAR']);

        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);
        $this->assertEvents($runId, [
            'storage.tableCreated',
            'storage.bucketRefreshed',
        ]);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $db->dropTable('TEST2');
        $db->executeQuery('ALTER TABLE "TEST" DROP COLUMN "AMOUNT"');
        $db->executeQuery('ALTER TABLE "TEST" ADD COLUMN "XXX" FLOAT');
        $db->createTable('TEST3', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'VARCHAR']);

        $runId = $this->setRunId();
        $this->_client->refreshBucket($idOfBucket);
        $this->assertEvents($runId, [
            'storage.tableDeleted',
            'storage.tableCreated',
            'storage.tableColumnsUpdated',
            'storage.bucketRefreshed',
        ]);

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

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
    }

    public function testRegisterExternalDB(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration-ext', true);

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
        $this->assertEvents($runId, [
            'storage.bucketCreated',
            'storage.tableCreated',
        ]);

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
