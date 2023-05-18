<?php

namespace Keboola\Test\Backend\Bigquery;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Metadata;

class TableDefinitionOperationsTest extends StorageApiTestCase
{
    use TestExportDataProvidersTrait;

    protected string $tableId;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->tableId = $this->createTableDefinition();
    }

    private function createTableDefinition(): string
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my_new_table',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'STRING',
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition($bucketId, $data);
    }


    public function testResponseDefinition(): void
    {
        $tableDetail = $this->_client->getTable($this->tableId);
        $this->assertSame([
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                    'basetype' => 'INTEGER',
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'STRING',
                        'nullable' => true,
                    ],
                    'basetype' => 'STRING',
                ],
            ],
        ], $tableDetail['definition']);
    }

    public function testPrimaryKeys(): void
    {
        $this->_client->dropTable($this->tableId);
        $bucketId = $this->getTestBucketId();

        // create table with PK on basetype defined column
        $data = [
            'name' => 'my_new_table',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'basetype' => 'STRING',
                ],
                [
                    'name' => 'name',
                    'basetype' => 'INTEGER',
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, $data);
        $this->assertNotNull($tableId);

        $m = new Metadata($this->_client);
        $this->assertTableColumnNullable($m, $tableId, 'id', false);
        $this->assertTableColumnNullable($m, $tableId, 'name', true);
    }

    public function testDataPreviewForTableDefinitionWithDecimalType(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3',
                    ],
                ],
                [
                    'name' => 'column_float',
                    'definition' => [
                        'type' => 'FLOAT64',
                    ],
                ],
                [
                    'name' => 'column_boolean',
                    'definition' => [
                        'type' => 'BOOL',
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
                        'type' => 'TIMESTAMP',
                    ],
                ],
                [
                    'name' => 'column_varchar',
                    'definition' => [
                        'type' => 'STRING',
                    ],
                ],
            ],
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '003.123',
                '3.14',
                0,
                '1989-08-31',
                '1989-08-31 00:00:00.000',
                'roman',
            ]
        );

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        /** @var array $data */
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
                    'value' => 'false',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_date',
                    'value' => '1989-08-31',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp',
                    'value' => '1989-08-31 00:00:00.000000+00:00',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_varchar',
                    'value' => 'roman',
                    'isTruncated' => false,
                ],
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);
    }

    public function testDataPreviewForTableDefinitionBaseType(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'id',
                    'basetype' => 'INTEGER',
                ],
                [
                    'name' => 'column_decimal',
                    'basetype' => 'NUMERIC',
                ],
                [
                    'name' => 'column_float',
                    'basetype' => 'FLOAT',
                ],
                [
                    'name' => 'column_boolean',
                    'basetype' => 'BOOLEAN',
                ],
                [
                    'name' => 'column_date',
                    'basetype' => 'DATE',
                ],
                [
                    'name' => 'column_timestamp',
                    'basetype' => 'TIMESTAMP',
                ],
                [
                    'name' => 'column_varchar',
                    'basetype' => 'STRING',
                ],
            ],
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '0.123',
                '3.14',
                0,
                '1989-08-31',
                '1989-08-31 00:00:00.000',
                'roman',
            ]
        );

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        /** @var array $data */
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
                    'value' => '0.123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float',
                    'value' => '3.14',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_boolean',
                    'value' => 'false',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_date',
                    'value' => '1989-08-31',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp',
                    'value' => '1989-08-31 00:00:00.000000+00:00',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_varchar',
                    'value' => 'roman',
                    'isTruncated' => false,
                ],
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);
    }

    public function testDataPreviewForTableDefinitionWithoutDefinitionAndBaseType(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'id',
                ],
                [
                    'name' => 'column_decimal',
                ],
                [
                    'name' => 'column_float',
                ],
                [
                    'name' => 'column_boolean',
                ],
                [
                    'name' => 'column_date',
                ],
                [
                    'name' => 'column_timestamp',
                ],
                [
                    'name' => 'column_varchar',
                ],
            ],
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '003.123',
                '3.14',
                0,
                '1989-08-31',
                '1989-08-31 00:00:00.000',
                'roman',
            ]
        );

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        /** @var array $data */
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
                    'value' => '003.123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float',
                    'value' => '3.14',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_boolean',
                    'value' => '0',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_date',
                    'value' => '1989-08-31',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp',
                    'value' => '1989-08-31 00:00:00.000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_varchar',
                    'value' => 'roman',
                    'isTruncated' => false,
                ],
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);
    }

    public function testAddTypedColumnOnNonTypedTable(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-non-typed',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'id',
                ],
            ],
        ];

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        try {
            $this->_client->addTableColumn($tableId, 'column_typed', [
                'type' => 'VARCHAR',
            ]);
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            $this->assertSame('Invalid parameters - definition: This field was not expected.', $e->getMessage());
            $this->assertSame('storage.tables.validation', $e->getStringCode());
        }
    }

    public function testTableWithDot(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'nameWith.Dot',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                    ],
                ],
            ],
        ];

        $this->expectException(ClientException::class);
        $this->_client->createTableDefinition($bucketId, $tableDefinition);
    }

    public function testAddColumnOnTypedTable(): void
    {
        $tableDefinition = [
            'name' => 'my-new-table-add-column',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3',
                    ],
                ],
            ],
        ];

        $sourceTableId = $this->_client->createTableDefinition($this->getTestBucketId(self::STAGE_IN), $tableDefinition);

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $sourceTableId, 'table-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $firstAliasTableId, 'table-2');

        $newColumns = [
            [
                'name' => 'column_float',
                'definition' => [
                    'type' => 'FLOAT64',
                ],
                'basetype' => null,
            ],
            [
                'name' => 'column_boolean',
                'definition' => [
                    'type' => 'BOOL',
                ],
                'basetype' => null,
            ],
            [
                'name' => 'column_date',
                'definition' => [
                    'type' => 'DATE',
                ],
                'basetype' => null,
            ],
            [
                'name' => 'column_timestamp',
                'definition' => [
                    'type' => 'TIMESTAMP',
                ],
                'basetype' => null,
            ],
            [
                'name' => 'column_string',
                'definition' => [
                    'type' => 'STRING',
                ],
                'basetype' => null,
            ],
            [
                'name' => 'basetype',
                'definition' => null,
                'basetype' => 'STRING',
            ],
        ];

        foreach ($newColumns as $newColumn) {
            $this->_client->addTableColumn($sourceTableId, $newColumn['name'], $newColumn['definition'], $newColumn['basetype']);
        }

        $expectedColumns = [
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_string',
            'basetype',
        ];
        $this->assertEquals($expectedColumns, $this->_client->getTable($sourceTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $addedColumnMetadata = $metadataClient->listColumnMetadata("{$sourceTableId}.column_float");
        // alias tables has metadata from source table
        $firstAliasAddedColumnMetadata = $this->_client->getTable($firstAliasTableId)['sourceTable']['columnMetadata']['column_float'];
        $secondAliasAddedColumnMetadata = $this->_client->getTable($secondAliasTableId)['sourceTable']['columnMetadata']['column_float'];

        foreach ([
                     $addedColumnMetadata,
                     $firstAliasAddedColumnMetadata,
                     $secondAliasAddedColumnMetadata,
                 ] as $columnMetadata) {
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.type',
                'value' => 'FLOAT64',
                'provider' => 'storage',
            ], $columnMetadata[0], ['id', 'timestamp']);
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.nullable',
                'value' => '1',
                'provider' => 'storage',
            ], $columnMetadata[1], ['id', 'timestamp']);
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.basetype',
                'value' => 'FLOAT',
                'provider' => 'storage',
            ], $columnMetadata[2], ['id', 'timestamp']);
        }
    }

    public function testAddTypedColumnToNonTypedTableShouldFail(): void
    {
        $tableDefinition = [
            'name' => 'my-new-table-typed-add-column',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3',
                    ],
                ],
            ],
        ];

        $sourceTableId = $this->_client->createTableDefinition($this->getTestBucketId(self::STAGE_IN), $tableDefinition);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Column "definition" or "basetype" must be set.');
        $this->_client->addTableColumn($sourceTableId, 'addColumn');
    }

    public function testDropColumnOnTypedTable(): void
    {
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $this->tableId, 'table-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $firstAliasTableId, 'table-2');

        $expectedColumns = ['id', 'name'];
        $this->assertEquals($expectedColumns, $this->_client->getTable($this->tableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);

        // force because table has aliases
        $this->_client->deleteTableColumn($this->tableId, 'name', ['force' => true]);

        $expectedColumns = ['id'];
        $this->assertEquals($expectedColumns, $this->_client->getTable($this->tableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);
    }

    public function testPrimaryKeyOperationsOnTypedTable(): void
    {
        $this->expectNotToPerformAssertions();
        $this->_client->removeTablePrimaryKey($this->tableId);
        $this->_client->createTablePrimaryKey($this->tableId, ['id']);
        $this->_client->removeTablePrimaryKey($this->tableId);
        // create composite primary key without data
        $this->_client->createTablePrimaryKey($this->tableId, ['id', 'name']);
        $this->_client->removeTablePrimaryKey($this->tableId);
        // load data with nulls
        $this->_client->writeTableAsync($this->tableId, new CsvFile(__DIR__ . '/../../_data/languages.null.csv'));
        // try to create composite primary key on column with nulls
        $this->_client->createTablePrimaryKey($this->tableId, ['id', 'name']);
        // Snowflake supports PK on nulls
    }

    public function testCreateSnapshotOnTypedTable(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);

        $idColumnMetadataBeforeSnapshot = $metadataClient->listColumnMetadata("{$this->tableId}.id");
        $nameColumnMetadataBeforeSnapshot = $metadataClient->listColumnMetadata("{$this->tableId}.name");

        $snapshotId = $this->_client->createTableSnapshot($this->tableId, 'table definition snapshot');

        $newTableId = $this->_client->createTableFromSnapshot($bucketId, $snapshotId, 'restored');
        $newTable = $this->_client->getTable($newTableId);
        $this->assertEquals('restored', $newTable['name']);

        $this->assertSame(['id'], $newTable['primaryKey']);
        $this->assertTrue($newTable['isTyped']);

        $this->assertSame(['id', 'name',], $newTable['columns']);

        $this->assertCount(1, $newTable['metadata']);

        $metadata = reset($newTable['metadata']);
        $this->assertSame('storage', $metadata['provider']);
        $this->assertSame('KBC.dataTypesEnabled', $metadata['key']);
        $this->assertSame('true', $metadata['value']);

        $idColumnMetadata = $metadataClient->listColumnMetadata("{$newTableId}.id");
        $nameColumnMetadata = $metadataClient->listColumnMetadata("{$newTableId}.name");

        // check that the new metadata has expected values
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'INT64',
            'provider' => 'storage',
        ], $idColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '',
            'provider' => 'storage',
        ], $idColumnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'INTEGER',
            'provider' => 'storage',
        ], $idColumnMetadata[2], ['id', 'timestamp']);

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'STRING',
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

        // check that the new table has datatype metadata same as the table before
        for ($i = 0; $i <= 2; $i++) {
            $this->assertArrayEqualsExceptKeys($idColumnMetadataBeforeSnapshot[$i], $idColumnMetadata[$i], [
                'id',
                'timestamp',
            ]);
            $this->assertArrayEqualsExceptKeys($nameColumnMetadataBeforeSnapshot[$i], $nameColumnMetadata[$i], [
                'id',
                'timestamp',
            ]);
        }
    }

    /**
     * @dataProvider wrongDatatypeFilterProvider
     */
    public function testColumnTypesInTableDefinition(array $params, string $expectExceptionMessage): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableId = $this->_client->createTableDefinition($bucketId, $this->getTestTableDefinitions());

        $this->_client->writeTableAsync($tableId, $this->getTestCsv());

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectExceptionMessage);
        $this->_client->getTableDataPreview($tableId, $params);
    }

    public function wrongDatatypeFilterProvider(): Generator
    {
        return $this->getWrongDatatypeFilters(['json', 'rfc']);
    }
}
