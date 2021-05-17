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

        $this->_client->createTableDefinition($bucketId, $data);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        //check that the job has started
        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        $this->assertSame($runId, $workspaceCreatedEvent['runId']);
        $this->assertSame('storage.tableDefinitionCreated', $workspaceCreatedEvent['event']);
        $this->assertSame('storage', $workspaceCreatedEvent['component']);
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
