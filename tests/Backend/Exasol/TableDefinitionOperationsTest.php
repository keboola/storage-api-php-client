<?php

namespace Keboola\Test\Backend\Exasol;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;

class TableDefinitionOperationsTest extends StorageApiTestCase
{
    private string $tableId;

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
                        'type' => 'DECIMAL',
                        'nullable' => false,
                        'length' => '18,0',
                    ],
                    'basetype' => 'NUMERIC',
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'VARCHAR',
                        'nullable' => true,
                        'length' => '2000000',
                    ],
                    'basetype' => 'STRING',
                ],
            ],
        ], $tableDetail['definition']);
    }

    public function testDataPreviewForTableDefinition(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $types = [
            'DECIMAL' => ['value' => '1'],
            'DOUBLE_PRECISION' => ['value' => '1'],
            'BOOLEAN' => ['value' => '1'],
            'DATE' => ['value' => '1'],
            'TIMESTAMP' => ['value' => '2021-01-01 00:00:00'],
            'TIMESTAMP_WITH_LOCAL_ZONE' => ['value' => '2021-01-01 00:00:00'],
            'INTERVAL_YEAR_TO_MONTH' => ['value' => '2021-01-01 00:00:00'],
            'INTERVAL_DAY_TO_SECOND' => ['value' => '2021-01-01 00:00:00'],
            'CHAR' => ['value' => '1'],
            'VARCHAR' => ['value' => '1'],
            'BIGINT' => ['value' => '1'],
            'INT' => ['value' => '1'],
            'INTEGER' => ['value' => '1'],
            'SHORTINT' => ['value' => '1'],
            'SMALLINT' => ['value' => '1'],
            'TINYINT' => ['value' => '1'],
            'BOOL' => ['value' => '1'],
            'CHAR_VARYING' => ['value' => '1'],
            'CHARACTER' => ['value' => '1'],
            'CHARACTER_LARGE_OBJECT' => ['value' => '1'],
            'CHARACTER_VARYING' => ['value' => '1'],
            'DEC' => ['value' => '1'],
            'LONG_VARCHAR' => ['value' => '1'],
            'NCHAR' => ['value' => '1'],
            'NUMBER' => ['value' => '1'],
            'NUMERIC' => ['value' => '1'],
            'NVARCHAR' => ['value' => '1'],
            'NVARCHAR2' => ['value' => '1'],
            'VARCHAR2' => ['value' => '1'],
            'DOUBLE' => ['value' => '1'],
            'FLOAT' => ['value' => '1'],
            'REAL' => ['value' => '1'],
        ];

        $whereFilters = [];
        $values = [];
        $columns = [];
        foreach ($types as $type => $options) {
            $columnName = 'column_' . str_replace(' ', '', strtolower($type));
            $values[$columnName] = $options['value'];
            $columns[] = [
                'name' => $columnName,
                'definition' => [
                    'type' => $type,
                ],
            ];
            $whereFilters[] = [
                'column' => $columnName,
                'operator' => 'eq',
                'values' => [$options['value']],
            ];
        }

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => $columns,
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow(array_keys($values));
        $csvFile->writeRow(array_values($values));

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $expectedPreview = [
            [

            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        // test filters
        foreach ($whereFilters as $filter) {
            /** @var array $data */
            $data = $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'json',
                    'whereFilters' => [$filter],
                ]
            );
            $this->assertCount(1, $data['rows'], sprintf('Filter for column %s failed.', $filter['column']));
        }
    }

    public function testFailedDataPreviewForTableDefinition(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $types = [
            'GEOMETRY',
            'HASHTYPE',
            'CLOB',
        ];

        $whereFilters = [];
        $columns = [];
        foreach ($types as $type) {
            $columnName = 'column_' . str_replace(' ', '', strtolower($type));
            $columns[] = [
                'name' => $columnName,
                'definition' => [
                    'type' => $type,
                ],
            ];
            $whereFilters[] = [
                'column' => $columnName,
                'operator' => 'eq',
                'values' => [''],
            ];
        }

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => $columns,
        ];

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $expectedPreview = [
            [
                // TODO: expect some columns not to be present or have some value placeholder
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );

        // test filters
        foreach ($whereFilters as $filter) {
            try {
                $this->_client->getTableDataPreview(
                    $tableId,
                    [
                        'format' => 'json',
                        'whereFilters' => [$filter],
                    ]
                );
                // fail
            } catch (ClientException $e) {
                $this->assertSame(400, $e->getCode());
                $this->assertSame('', $e->getStringCode());
                $this->assertSame('', $e->getMessage());
            }
        }
    }

    public function testDataPreviewForTableDefinitionBaseType(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-for-data-preview',
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
                '0.123', // default is (36,36) => 1e-36
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
            'name' => 'my-new-table-for-data-preview',
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
                    'type' => 'FLOAT',
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
                'name' => 'column_varchar',
                'definition' => [
                    'type' => 'VARCHAR',
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
            'column_varchar',
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

    public function testCreateSnapshotOnTypedTable(): void
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
                'name',
            ],
            $newTable['columns']
        );

        $this->assertCount(1, $newTable['metadata']);

        $metadata = reset($newTable['metadata']);
        $this->assertSame('storage', $metadata['provider']);
        $this->assertSame('KBC.dataTypesEnabled', $metadata['key']);
        $this->assertSame('true', $metadata['value']);
        $this->assertTrue($newTable['isTyped']);

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

    /**
     * @dataProvider  filterProvider
     */
    public function testColumnTypesInTableDefinition(array $params, string $expectExceptionMessage): void
    {
        $bucketId = $this->getTestBucketId();

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'column_int',
                    'definition' => [
                        'type' => 'INT',
                    ],
                ],
                [
                    'name' => 'column_number',
                    'definition' => [
                        'type' => 'NUMBER',
                    ],
                ],
                [
                    'name' => 'column_float',
                    'definition' => [
                        'type' => 'FLOAT',
                    ],
                ],
                [
                    'name' => 'column_varchar',
                    'definition' => [
                        'type' => 'VARCHAR',
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
                    'name' => 'column_boolean',
                    'definition' => [
                        'type' => 'BOOLEAN',
                    ],
                ],
            ],
        ];

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow([
            'column_int',
            'column_number',
            'column_float',
            'column_varchar',
            'column_date',
            'column_timestamp',
            'column_boolean',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '3.14',
                '3.14',
                'Jirka :D',
                '1989-08-31',
                '2023-04-18 12:34:56',
                0,
            ]
        );

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectExceptionMessage);
        $this->_client->getTableDataPreview($tableId, $params);
    }

    public function filterProvider(): Generator
    {
        foreach (['json', 'rfc'] as $format) {
            yield 'overflow int '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_int',
                            'operator' => 'gt',
                            'values' => ['999999999999999999999999999999999999999'],
                        ],
                    ],
                ],
                '[EXASOL] GlobalTransactionRollback msg: data exception - string data, right truncation. ',
            ];

            yield 'wrong int '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_int',
                            'operator' => 'eq',
                            'values' => ['aaa'],
                        ],
                    ],
                ],
                '[EXASOL] data exception - invalid character value for cast; Value: \'aaa\' ',
            ];

            yield 'wrong number '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_number',
                            'operator' => 'eq',
                            'values' => ['aaa'],
                        ],
                    ],
                ],
                '[EXASOL] data exception - invalid character value for cast; Value: \'aaa\' ',
            ];

            yield 'wrong float '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_float',
                            'operator' => 'eq',
                            'values' => ['aaa'],
                        ],
                    ],
                ],
                '[EXASOL] data exception - invalid character value for cast; Value: \'aaa\' ',
            ];

            yield 'wrong boolean '. $format => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'column_boolean',
                            'operator' => 'eq',
                            'values' => ['222'],
                        ],
                    ],
                ],
                '[EXASOL] GlobalTransactionRollback msg: data exception - string data, right truncation. ',
            ];

            yield 'wrong date '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_date',
                            'operator' => 'eq',
                            'values' => ['12:00:00.000'],
                        ],
                    ],
                ],
                '[EXASOL] GlobalTransactionRollback msg: data exception - string data, right truncation. ',
            ];

            yield 'wrong timestamp '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_timestamp',
                            'operator' => 'eq',
                            'values' => ['xxx'],
                        ],
                    ],
                ],
                '[EXASOL] data exception - invalid value for YYYY format token; Value: \'xxx\' Format: \'YYYY-MM-DD HH24:MI:SS.FF6\' ',
            ];
        }
    }
}
