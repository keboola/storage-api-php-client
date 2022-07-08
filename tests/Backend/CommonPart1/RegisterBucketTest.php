<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;

class RegisterBucketTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testRegisterBucket(): void
    {
        $token = $this->_client->verifyToken();

        if (!in_array('input-mapping-read-only-storage', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Read only mapping is not enabled for project "%s"', $token['owner']['id']));
        }
        if (!in_array('external-buckets', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('External buckets are not enabled for project "%s"', $token['owner']['id']));
        }
        if (!in_array(
            $token['owner']['defaultBackend'],
            [
                self::BACKEND_SNOWFLAKE,
            ],
            true
        )) {
            self::markTestSkipped(sprintf(
                'Backend "%s" is not supported external bucket registration',
                $token['owner']['defaultBackend']
            ));
        }

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
        } catch (ClientException $e) {
            $this->assertSame('storage.schemaNotFound', $e->getStringCode());
            $this->assertSame(
                'Schema "non-existing-schema" doesn\'t exist or project user is missing privileges to read from it.',
                $e->getMessage()
            );
        }

        $ws = new Workspaces($this->_client);

        $workspace = $ws->createWorkspace();

        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration',
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            'in',
            'Iam in workspace',
            'snowflake',
            'Iam-your-workspace'
        );

        $bucket = $this->_client->getBucket($idOfBucket);
        $this->assertTrue($bucket['hasExternalSchema']);
        $this->assertSame($workspace['connection']['database'], $bucket['databaseName']);

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(0, $tables);

        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $db->createTable('TEST', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);
        $db->executeQuery('INSERT INTO "TEST" VALUES (1, \'test\')');
        $this->_client->refreshBucket($idOfBucket);

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
        $this->_client->refreshBucket($idOfBucket);
        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(2, $tables);

        $db->dropTable('TEST2');
        $db->executeQuery('ALTER TABLE "TEST" DROP COLUMN "AMOUNT"');
        $db->executeQuery('ALTER TABLE "TEST" ADD COLUMN "XXX" FLOAT');
        $db->createTable('TEST3', ['AMOUNT' => 'NUMBER', 'DESCRIPTION' => 'TEXT']);

        $this->_client->refreshBucket($idOfBucket);
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

        $this->dropBucketIfExists($this->_client, 'in.test-bucket-registration-ext', [
            'force' => true,
            'async' => true,
        ]);
        // try same with schema outside of project database
        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration-ext',
            ['TEST_EXTERNAL_BUCKETS', 'TEST_SCHEMA'],
            'in',
            'Iam in other database',
            'snowflake',
            'Iam-from-external-db-ext'
        );
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

        // check that workspace user can read from table in external bucket directly
        assert($db instanceof SnowflakeWorkspaceBackend);
        $result = $db->getDb()->fetchAll('SELECT COUNT(*) AS CNT FROM "TEST_EXTERNAL_BUCKETS"."TEST_SCHEMA"."TEST_TABLE"');
        $this->assertSame([
            [
                'CNT' => '1',
            ],
        ], $result);

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
    }

    private function assertColumnMetadata(
        string $expectedType,
        string $expectedNullable,
        string $expectedBasetype,
        ?string $expectedLength,
        array $columnMetadata
    ): void {
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => $expectedType,
            'provider' => 'storage',
        ], $columnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => $expectedNullable,
            'provider' => 'storage',
        ], $columnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => $expectedBasetype,
            'provider' => 'storage',
        ], $columnMetadata[2], ['id', 'timestamp']);

        if ($expectedLength !== null) {
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.length',
                'value' => $expectedLength,
                'provider' => 'storage',
            ], $columnMetadata[3], ['id', 'timestamp']);
        }
    }
}
