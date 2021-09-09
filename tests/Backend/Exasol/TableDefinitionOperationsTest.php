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
        ]);
        $csvFile->writeRow(['1', '003.123', '3.14', 0, '1989-08-31', '1989-08-31 00:00:00.000', 'roman']);


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
                    'value' =>  '1989-08-31 00:00:00.000000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_varchar',
                    'value' =>  'roman',
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
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->deleteTableColumn($this->tableId, 'name');
    }

    public function testAddPrimaryKeyOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->createTablePrimaryKey($this->tableId, ['id']);
    }
    public function testRemovePrimaryKeyOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->removeTablePrimaryKey($this->tableId);
    }

    public function testCreateSnapshotOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->createTableSnapshot($this->tableId);
    }
}
