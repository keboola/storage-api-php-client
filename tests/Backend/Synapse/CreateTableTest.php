<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;

class CreateTableTest extends StorageApiTestCase
{
    const TABLE_DEFINITION = [
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

    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testDataPreviewForTableDefinitionWithDecimalType()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
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
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3'
                    ],
                ],
                [
                    'name' => 'column_float',
                    'definition' => [
                        'type' => 'FLOAT',
                    ],
                ],
                [
                    'name' => 'column_boolean',
                    'definition' => [
                        'type' => 'BIT',
                    ],
                ],
                [
                    'name' => 'column_date',
                    'definition' => [
                        'type' => 'DATE',
                    ],
                ],
                [
                    'name' => 'column_timestamp',
                    'definition' => [
                        'type' => 'TIME',
                    ],
                ],
                [
                    'name' => 'column_varchar',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'name' => 'column_money',
                    'definition' => [
                        'type' => 'MONEY',
                    ],
                ],
                [
                    'name' => 'column_small_money',
                    'definition' => [
                        'type' => 'SMALLMONEY',
                    ],
                ],
                [
                    'name' => 'column_uniq',
                    'definition' => [
                        'type' => 'UNIQUEIDENTIFIER',
                    ],
                ],
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];

        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow([
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
            'column_money',
            'column_small_money',
            'column_uniq',
            ]);
        $csvFile->writeRow(['1', '003.123', '3.14', 1, '1989-08-31', '05:00:01', 'roman', '3148.29', '3148.29', '0E984725-C51C-4BF4-9960-E1C80E27ABA0']);


        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTable($tableId, $csvFile);

        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $expectedPreview = [
            [
                [
                    'columnName' => 'id',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_decimal',
                    'value' => '3.123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float',
                    'value' => '3.14',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_boolean',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_date',
                    'value' => '1989-08-31',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp',
                    'value' => '05:00:01.0000000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_varchar',
                    'value' => 'roman',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_money',
                    'value' => '3148.29',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_small_money',
                    'value' => '3148.29',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_uniq',
                    'value' => '0E984725-C51C-4BF4-9960-E1C80E27ABA0',
                    'isTruncated' => false,
                ],
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertSame(1, count($data['rows']));

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertSame(1, count($data['rows']));
    }

    public function testSourceTableColumnAddWithAutoSyncAliases()
    {
        $tableDefinition = [
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
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3'
                    ],
                ],
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];

        $sourceTableId = $this->_client->createTableDefinition($this->getTestBucketId(self::STAGE_IN), $tableDefinition);

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $sourceTableId, 'table-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $firstAliasTableId, 'table-2');

        $this->_client->addTableColumn($sourceTableId, 'added_column', [
            'type' => 'DECIMAL',
            'length' => '4,3'
        ]);

        $expectedColumns = ['id', 'column_decimal', 'added_column'];
        $this->assertEquals($expectedColumns, $this->_client->getTable($sourceTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $addedColumnMetadata = $metadataClient->listColumnMetadata("{$sourceTableId}.added_column");
        // alias tables has metadata from source table
        $firstAliasAddedColumnMetadata = $this->_client->getTable($firstAliasTableId)['sourceTable']['columnMetadata']['added_column'];
        $secondAliasAddedColumnMetadata = $this->_client->getTable($secondAliasTableId)['sourceTable']['columnMetadata']['added_column'];

        foreach ([$addedColumnMetadata, $firstAliasAddedColumnMetadata, $secondAliasAddedColumnMetadata] as $columnMetadata) {
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.type',
                'value' => 'DECIMAL',
                'provider' => 'storage',
            ], $columnMetadata[0], ['id', 'timestamp']);
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.nullable',
                'value' => '1',
                'provider' => 'storage',
            ], $columnMetadata[1], ['id', 'timestamp']);
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.basetype',
                'value' => 'NUMERIC',
                'provider' => 'storage',
            ], $columnMetadata[2], ['id', 'timestamp']);
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.length',
                'value' => '4,3',
                'provider' => 'storage',
            ], $columnMetadata[3], ['id', 'timestamp']);
        }
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

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, self::TABLE_DEFINITION);

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

    public function testCreateTableDefinitionNoPrimaryKey()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $definition = self::TABLE_DEFINITION;
        // remove primaryKeysNames, try if request is validated
        unset($definition['primaryKeysNames']);

        try {
            $this->_client->createTableDefinition($bucketId, $definition);
            self::fail('Table should not be created.');
        } catch (ClientException $e) {
            self::assertSame('Invalid request', $e->getMessage());
        }

        $definition = self::TABLE_DEFINITION;
        $definition['primaryKeysNames'] = [];
        $definition['distribution'] = [
            'type' => 'ROUND_ROBIN',
            'distributionColumnsNames' => [],
        ];

        $this->_client->createTableDefinition($bucketId, $definition);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        //check that the job has started
        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        $workspaceCreatedEvent = array_pop($events);
        self::assertSame($runId, $workspaceCreatedEvent['runId']);
        self::assertSame('storage.tableCreated', $workspaceCreatedEvent['event']);
        self::assertSame('storage', $workspaceCreatedEvent['component']);

        // event params validation
        $eventParams = $workspaceCreatedEvent['params'];

        // empty PK's
        self::assertSame([], $eventParams['primaryKey']);
        self::assertSame([], $eventParams['distributionKey']);
        self::assertSame('ROUND_ROBIN', $eventParams['distribution']);
    }

    public function testColumnTypesInTableDefinition()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, self::TABLE_DEFINITION);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $idColumnMetadata = $metadataClient->listColumnMetadata("{$tableId}.id");
        $nameColumnMetadata = $metadataClient->listColumnMetadata("{$tableId}.name");

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'INT',
            'provider' => 'storage',
        ], $idColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
            'provider' => 'storage',
        ], $idColumnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'INTEGER',
            'provider' => 'storage',
        ], $idColumnMetadata[2], ['id', 'timestamp']);

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'NVARCHAR',
            'provider' => 'storage',
        ], $nameColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
            'provider' => 'storage',
        ], $nameColumnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'STRING',
            'provider' => 'storage',
        ], $nameColumnMetadata[2], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.length',
            'value' => '4000',
            'provider' => 'storage',
        ], $nameColumnMetadata[3], ['id', 'timestamp']);
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
