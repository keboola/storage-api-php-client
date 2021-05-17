<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class CreateTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testCreateTableWithDistributionKey()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        // create table with distributionKey
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            [
                'distributionKey' => 'name',
            ]
        );

        $table = $this->_client->getTable($tableId);
        self::assertEquals(['name'], $table['distributionKey']);

        // create table with primaryKey
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages2',
            new CsvFile($importFile),
            [
                'primaryKey' => 'name',
            ]
        );

        $table = $this->_client->getTable($tableId);
        self::assertEquals(['name'], $table['distributionKey']);

        try {
            $this->_client->createTableAsync(
                $bucketId,
                'languages',
                new CsvFile($importFile),
                [
                    'distributionKey' => ['name', 'id'],
                ]
            );
            self::fail('distributions keys send as array should throw exception');
        } catch (ClientException $e) {
            self::assertEquals(
            // phpcs:ignore
                'distributionKey must be string. Use comma as separator for multiple distribution keys.',
                $e->getMessage()
            );
            self::assertEquals(
                'storage.validation.distributionKey',
                $e->getStringCode()
            );
        }

        try {
            $this->_client->createTableAsync(
                $bucketId,
                'languages',
                new CsvFile($importFile),
                [
                    'distributionKey' => 'name,id',
                ]
            );
            self::fail('Multiple distributions keys should throw exception');
        } catch (ClientException $e) {
            self::assertEquals(
                'Synapse backend only supports one distributionKey.',
                $e->getMessage()
            );
            self::assertEquals(
                'storage.validation.distributionKey',
                $e->getStringCode()
            );
        }
    }

    public function testCreateTableWithCrlfLineEndings()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.crlf.csv';
        // create table with distributionKey
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile)
        );

        $importedData = $this->_client->getTableDataPreview($tableId);
        $this->assertCount(5, Client::parseCsv($importedData));
    }

    public function testCreateTableDefinition()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my-new-table',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'NVARCHAR',
                    ],
                ],
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, $data);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        //check that the job has started
        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.tableCreated', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);

        // event params validation
        $eventParams = $workspaceCreatedEvent['params'];

        $this->assertSame(['id'], $eventParams['primaryKey']);
        $this->assertSame(
            [
                'id',
                'name',
            ],
            $eventParams['columns']
        );
        $this->assertSame(
            [
                'id' => [
                    'type' => 'INT',
                    'length' => null,
                    'nullable' => true,
                ],
                'name' => [
                    'type' => 'NVARCHAR',
                    'length' => null,
                    'nullable' => true,
                ],
            ],
            $eventParams['columnsTypes']
        );
        $this->assertSame(false, $eventParams['syntheticPrimaryKeyEnabled']);
        $this->assertSame(['id'], $eventParams['distributionKey']);
        $this->assertSame('HASH', $eventParams['distribution']);

        // table properties validation
        $table = $this->_client->getTable($tableId);

        $this->assertSame('my-new-table', $table['name']);
        $this->assertSame('my-new-table', $table['displayName']);

        $this->assertSame(['id'], $table['primaryKey']);
        $this->assertSame(['id'], $table['distributionKey']);
        $this->assertSame(['id'], $table['indexedColumns']);

        $this->assertSame(false, $table['syntheticPrimaryKeyEnabled']);

        $this->assertSame(
            [
                'id',
                'name'
            ],
            $table['columns']
        );

        $this->assertCount(1, $table['metadata']);

        $metadata = reset($table['metadata']);
        $this->assertSame('storage', $metadata['provider']);
        $this->assertSame('KBC.dataTypesEnabled', $metadata['key']);
        $this->assertSame('true', $metadata['value']);
    }

    public function testCreateTableDefinitionWithWrongInput()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my-new-table',
            'primaryKeysNames' => ['notInColumns'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                    ],
                ],
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];
        try {
            $this->_client->createTableDefinition($bucketId, $data);
        } catch (ClientException $exception) {
            $this->assertEquals($exception->getStringCode(), 'validation.failed');
        }
    }
}
