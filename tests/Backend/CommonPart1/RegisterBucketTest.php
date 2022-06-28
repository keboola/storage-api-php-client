<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
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

        $idOfBucket = $this->_client->registerBucket(
            'test-bucket-registration',
            ['TEST_EXTERNAL_BUCKETS', 'TEST_SCHEMA'],
            'in',
            'Iam in other database',
            'snowflake',
            'Iam-from-external-db'
        );

        $tables = $this->_client->listTables($idOfBucket);
        $this->assertCount(1, $tables);

        $tableDetail = $this->_client->getTable($tables[0]['id']);

        $this->assertSame('KBC.dataTypesEnabled', $tableDetail['metadata'][0]['key']);
        $this->assertSame('true', $tableDetail['metadata'][0]['value']);

        $this->assertCount(2, $tableDetail['columns']);

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'NUMBER',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['AMOUNT'][0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['AMOUNT'][1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'NUMERIC',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['AMOUNT'][2], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.length',
            'value' => '38,0',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['AMOUNT'][3], ['id', 'timestamp']);

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'VARCHAR',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['DESCRIPTION'][0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['DESCRIPTION'][1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'STRING',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['DESCRIPTION'][2], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.length',
            'value' => '16777216',
            'provider' => 'storage',
        ], $tableDetail['columnMetadata']['DESCRIPTION'][3], ['id', 'timestamp']);

        $this->_client->exportTableAsync($tables[0]['id']);

        $preview = $this->_client->getTableDataPreview($tables[0]['id']);
        // expect two lines in preview because of the header
        $this->assertCount(2, Client::parseCsv($preview, false));

        $ws = new Workspaces($this->_client);
        $workspace = $ws->createWorkspace();

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
                'Table "test-bucket-registration" is part of external bucket "in.test-bucket-registration.TEST_TABLE" and cannot be loaded into workspace.',
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
                'Table "test-bucket-registration" is part of external bucket "in.test-bucket-registration.TEST_TABLE" and cannot be loaded into workspace.',
                $e->getMessage()
            );
        }

        $this->_client->dropBucket($idOfBucket, ['force' => true, 'async' => true]);
    }
}
