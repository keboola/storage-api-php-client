<?php

namespace Keboola\Test\Backend\Exasol;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;

class TableDefinitionOperationsTest extends StorageApiTestCase
{
    private $tableId;

    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();

        $this->tableId = $this->createTableDefinition();
    }

    private function createTableDefinition()
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
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'NVARCHAR',
                    ],
                ],
            ]
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition($bucketId, $data);
    }

    public function testDataPreviewForTableDefinitionWithDecimalType()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-for-data-preview',
            'primaryKeysNames' => [],
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
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'name' => 'column_interval_year_to_month',
                    'definition' => [
                        'type' => 'INTERVAL YEAR TO MONTH',
                        'length' => '3',
                    ],
                ],
                [
                    'name' => 'column_interval_day_to_second',
                    'definition' => [
                        'type' => 'INTERVAL DAY TO SECOND',
                        'length' => '4,5',
                    ],
                ],
                [
                    'name' => 'column_interval_year_to_month_default',
                    'definition' => [
                        'type' => 'INTERVAL YEAR TO MONTH',
                    ],
                ],
                [
                    'name' => 'column_interval_day_to_second_default',
                    'definition' => [
                        'type' => 'INTERVAL DAY TO SECOND',
                    ],
                ],
                [
                    'name' => 'column_geometry',
                    'definition' => [
                        'type' => 'GEOMETRY',
                        'length' => '8000000',
                    ],
                ],
                [
                    'name' => 'column_hashtype',
                    'definition' => [
                        'type' => 'HASHTYPE',
                        'length' => '16 BYTE',
                    ],
                ],
            ]
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
            'column_interval_year_to_month',
            'column_interval_day_to_second',
            'column_interval_year_to_month_default',
            'column_interval_day_to_second_default',
            'column_geometry',
            'column_hashtype',
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
                '5-3', // 'column_interval_year_to_month',
                '2 12:50:10.123', // 'column_interval_day_to_second',
                '5-3', // 'column_interval_year_to_month_default',
                '2 12:50:10.123', // 'column_interval_day_to_second_default',
                'POINT(2 5)', // 'column_geometry',
                '550e8400-e29b-11d4-a716-446655440000', // 'column_hashtype',
            ]
        );

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
                    'value' => 'FALSE',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_date',
                    'value' => '1989-08-31',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp',
                    'value' => '1989-08-31 00:00:00.000000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_varchar',
                    'value' => 'roman',
                    'isTruncated' => false,
                ],
                [
                    // 3
                    'columnName' => 'column_interval_year_to_month',
                    'value' => '+005-03',
                    'isTruncated' => false,
                ],
                [
                    // 4,5
                    'columnName' => 'column_interval_day_to_second',
                    'value' => '+0002 12:50:10.12300',
                    'isTruncated' => false,
                ],
                [
                    // default is 2
                    'columnName' => 'column_interval_year_to_month_default',
                    'value' => '+05-03',
                    'isTruncated' => false,
                ],
                [
                    // default is 2,3
                    'columnName' => 'column_interval_day_to_second_default',
                    'value' => '+02 12:50:10.123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_geometry',
                    'value' => 'POINT (2 5)',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_hashtype',
                    'value' => '550e8400e29b11d4a716446655440000',
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

        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);
    }

    public function testTableWithDot()
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

    public function testAddColumnOnTypedTable()
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
                        'length' => '4,3'
                    ],
                ],
            ],
        ];

        $sourceTableId = $this->_client->createTableDefinition($this->getTestBucketId(self::STAGE_IN), $tableDefinition);

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $sourceTableId, 'table-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $firstAliasTableId, 'table-2');

        $newColumns =  [
            [
                'name' => 'column_float',
                'definition' => [
                    'type' => 'FLOAT',
                ]
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
                    'type' => 'VARCHAR',
                ],
            ],
        ];

        foreach ($newColumns as $newColumn) {
            $this->_client->addTableColumn($sourceTableId, $newColumn['name'], $newColumn['definition']);
        }

        $expectedColumns = ['id', 'column_decimal', 'column_float', 'column_boolean', 'column_date', 'column_timestamp', 'column_varchar'];
        $this->assertEquals($expectedColumns, $this->_client->getTable($sourceTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $addedColumnMetadata = $metadataClient->listColumnMetadata("{$sourceTableId}.column_float");
        // alias tables has metadata from source table
        $firstAliasAddedColumnMetadata = $this->_client->getTable($firstAliasTableId)['sourceTable']['columnMetadata']['column_float'];
        $secondAliasAddedColumnMetadata = $this->_client->getTable($secondAliasTableId)['sourceTable']['columnMetadata']['column_float'];

        foreach ([$addedColumnMetadata, $firstAliasAddedColumnMetadata, $secondAliasAddedColumnMetadata] as $columnMetadata) {
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.type',
                'value' => 'DOUBLE PRECISION',
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
            $this->assertArrayEqualsExceptKeys([
                'key' => 'KBC.datatype.length',
                'value' => '64',
                'provider' => 'storage',
            ], $columnMetadata[3], ['id', 'timestamp']);
        }
    }

    public function testAddTypedColumnToNonTypedTableShouldFail()
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
                        'length' => '4,3'
                    ],
                ],
            ],
        ];

        $sourceTableId = $this->_client->createTableDefinition($this->getTestBucketId(self::STAGE_IN), $tableDefinition);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid parameters - definition: This field is missing.');
        $this->_client->addTableColumn($sourceTableId, 'addColumn');
    }

    public function testDropColumnOnTypedTable()
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

    public function testPrimaryKeyOperationsOnTypedTable()
    {
        $this->_client->removeTablePrimaryKey($this->tableId);
        $this->_client->createTablePrimaryKey($this->tableId, ['id']);
        $this->_client->removeTablePrimaryKey($this->tableId);
        // create composite primary key without data
        $this->_client->createTablePrimaryKey($this->tableId, ['id', 'name']);
        $this->_client->removeTablePrimaryKey($this->tableId);
        // load data with nulls
        $this->_client->writeTableAsync($this->tableId, new CsvFile(__DIR__ . '/../../_data/languages.null.csv'));
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('[EXASOL] constraint violation - not null');
        // try to create composite primary key on column with nulls
        $this->_client->createTablePrimaryKey($this->tableId, ['id', 'name']);
    }

    public function testCreateSnapshotOnTypedTable()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $snapshotId = $this->_client->createTableSnapshot($this->tableId, 'table definition snapshot');

        $newTableId = $this->_client->createTableFromSnapshot($bucketId, $snapshotId, 'restored');
        $newTable = $this->_client->getTable($newTableId);
        $this->assertEquals('restored', $newTable['name']);

        $this->assertSame(['id'], $newTable['primaryKey']);

        $this->assertSame(
            [
                'id',
                'name'
            ],
            $newTable['columns']
        );

        $this->assertCount(1, $newTable['metadata']);

        $metadata = reset($newTable['metadata']);
        $this->assertSame('storage', $metadata['provider']);
        $this->assertSame('KBC.dataTypesEnabled', $metadata['key']);
        $this->assertSame('true', $metadata['value']);

        // check that the new table has correct datypes in metadata
        $metadataClient = new Metadata($this->_client);
        $idColumnMetadata = $metadataClient->listColumnMetadata("{$newTableId}.id");
        $nameColumnMetadata = $metadataClient->listColumnMetadata("{$newTableId}.name");

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'DECIMAL',
            'provider' => 'storage',
        ], $idColumnMetadata[0], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '',
            'provider' => 'storage',
        ], $idColumnMetadata[1], ['id', 'timestamp']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'NUMERIC',
            'provider' => 'storage',
        ], $idColumnMetadata[2], ['id', 'timestamp']);

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'VARCHAR',
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
            'value' => '2000000',
            'provider' => 'storage',
        ], $nameColumnMetadata[3], ['id', 'timestamp']);
    }
}
