<?php

namespace Keboola\Test\Backend\Bigquery;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\Test\Backend\Workspaces\Backend\BigqueryWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use PHPUnit\Framework\AssertionFailedError;
use Throwable;

class TableDefinitionOperationsTest extends ParallelWorkspacesTestCase
{
    use TestExportDataProvidersTrait;

    protected string $tableId;

    public function setUp(): void
    {
        parent::setUp();
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

    public function testDataPreviewExoticTypes(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'exotic_my_new_table',
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
                    'name' => 'array',
                    'definition' => [
                        'type' => 'ARRAY',
                        'length' => 'INT64',
                    ],
                ],
                [
                    'name' => 'struct',
                    'definition' => [
                        'type' => 'STRUCT',
                        'length' => 'a INT64, b STRING',
                    ],
                ],
                [
                    'name' => 'bytes',
                    'definition' => [
                        'type' => 'BYTES',
                    ],
                ],
                [
                    'name' => 'geography',
                    'definition' => [
                        'type' => 'GEOGRAPHY',
                    ],
                ],
                [
                    'name' => 'interval',
                    'definition' => [
                        'type' => 'INTERVAL',
                    ],
                ],
                [
                    'name' => 'json',
                    'definition' => [
                        'type' => 'JSON',
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $workspace = $this->initTestWorkspace('bigquery');

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');

        /** @var BigQueryClient $db */
        $db = $backend->getDb();

        $qb = new BigqueryTableQueryBuilder();
        $db->runQuery($db->query($qb->getCreateTableCommand(
            $workspace['connection']['schema'],
            'test_Languages3',
            new ColumnCollection([
                new BigqueryColumn('id', new Bigquery('INT64')),
                new BigqueryColumn('array', new Bigquery('ARRAY', ['length' => 'INT64'])),
                new BigqueryColumn('struct', new Bigquery('STRUCT', ['length' => 'a INT64, b STRING'])),
                new BigqueryColumn('bytes', new Bigquery('BYTES')),
                new BigqueryColumn('geography', new Bigquery('GEOGRAPHY')),
                new BigqueryColumn('interval', new Bigquery('INTERVAL')),
                new BigqueryColumn('json', new Bigquery('JSON')),
            ]),
        )));
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            '
INSERT INTO %s.`test_Languages3` (`id`, `array`, `struct`, `bytes`, `geography`, `interval`, `json`) VALUES 
(1, [2,2], STRUCT(111, "roman"), b\'\x01\x02\x03\x04\', ST_GEOGPOINT(-122.4194, 37.7749), INTERVAL 1 YEAR, JSON\'{"a": 1, "b": 2}\') 
;',
            $workspace['connection']['schema'],
        ));

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
        ]);

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
                    'columnName' => 'array',
                    'value' => '[2,2]',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'struct',
                    'value' => '{"a":111,"b":"roman"}',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'bytes',
                    'value' => '',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'geography',
                    'value' => '{ "type": "Point", "coordinates": [-122.4194, 37.7749] } ',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'interval',
                    'value' => '1-0 0 0:0:0',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'json',
                    'value' => '{"a":1,"b":2}',
                    'isTruncated' => false,
                ],
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows'],
        );

        // filters
        foreach ($tableDefinition['columns'] as $col) {
            if ($col['name'] === 'id') {
                continue;
            }
            $filter = [
                'column' => $col['name'],
                'operator' => 'eq',
                'values' => [''],
            ];
            try {
                $this->_client->getTableDataPreview(
                    $tableId,
                    [
                        'format' => 'json',
                        'whereFilters' => [$filter],
                    ],
                );
                // fail
            } catch (ClientException $e) {
                $this->assertSame(400, $e->getCode());
                $this->assertSame('storage.backend.exception', $e->getStringCode());
                $this->assertSame(
                    sprintf(
                        'Filtering by column "%s" of type "%s" is not supported by the backend "bigquery".',
                        $col['name'],
                        $col['definition']['type'],
                    ),
                    $e->getMessage(),
                );
            }
        }
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
                        'type' => 'INTEGER',
                        'nullable' => false,
                    ],
                    'basetype' => 'INTEGER',
                    'canBeFiltered' => true,
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'STRING',
                        'nullable' => true,
                    ],
                    'basetype' => 'STRING',
                    'canBeFiltered' => true,
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
                    'definition' => [
                        'type' => 'STRING',
                    ],
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
            ],
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
            $data['rows'],
        );

        $this->assertCount(1, $data['rows']);

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows'],
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
            ],
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
            $data['rows'],
        );

        $this->assertCount(1, $data['rows']);

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows'],
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
            ],
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
            $data['rows'],
        );

        $this->assertCount(1, $data['rows']);

        //test types is provided from source table for alias
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $tableId, 'table-1');

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($firstAliasTableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows'],
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
                        'type' => 'INTEGER',
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
                        'type' => 'INTEGER',
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
            'value' => 'INTEGER',
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

    public function testLoadingNullValuesToNotNullTypedColumnsFromFile(): void
    {
        $bucketId = $this->getTestBucketId();

        $data = [
            'name' => 'my_new_table_with_nulls',
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
                    'name' => 'transid',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'item',
                    'definition' => [
                        'type' => 'STRING',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'price',
                    'definition' => [
                        'type' => 'NUMERIC',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'quantity',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, $data);

        $importFile = new CsvFile(__DIR__ . '/../../_data/transactions-nullify.csv');

        try {
            $this->_client->writeTableAsync($tableId, $importFile);
            $this->fail('should fail because of null value in not nullable column');
        } catch (ClientException $e) {
            $this->assertEquals('Load error: Required field quantity cannot be null', $e->getMessage());
        }
    }

    public function testLoadingNullValuesToNotNullTypedColumnsFromTable(): void
    {
        $bucketId = $this->getTestBucketId();

        $data = [
            'name' => 'my_new_table_with_nulls',
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
                    'name' => 'notnullcolumn',
                    'definition' => [
                        'type' => 'INT64',
                        'nullable' => false,
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $tableId = $this->_client->createTableDefinition($bucketId, $data);

        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        assert($backend instanceof BigqueryWorkspaceBackend);

        $client = $backend->getDb();
        $dataset = $workspace['connection']['schema'];

        // create table with null values in WS
        $client->runQuery(
            $client->query(
                sprintf(
                    'CREATE OR REPLACE TABLE `%s`.`my_new_table_with_nulls` (id INT64, notnullcolumn INT64);',
                    $dataset,
                ),
            ),
        );
        $client->runQuery(
            $client->query(
                sprintf(
                    'INSERT INTO `%s`.`my_new_table_with_nulls` VALUES (1, null);',
                    $dataset,
                ),
            ),
        );

        try {
            $this->_client->writeTableAsyncDirect($tableId, [
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'my_new_table_with_nulls',
                'incremental' => false,
            ]);
            $this->fail('should fail because of null value in not nullable column');
        } catch (ClientException $e) {
            $this->assertEquals('Load error: Required field notnullcolumn cannot be null', $e->getMessage());
        }
    }

    public function testInsertInvalidTypestampIsUserError(): void
    {
        $bucketId = $this->getTestBucketId();

        $data = [
            'name' => 'test-table',
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
                    'name' => 'timestamp',
                    'definition' => [
                        'type' => 'TIMESTAMP',
                        'nullable' => false,
                    ],
                ],
            ],
        ];

        $tableId = $this->_client->createTableDefinition($bucketId, $data);

        $data = [
            ['id', 'timestamp'],
            [1, '00:00:00',],
        ];

        $csvFile = $this->createTempCsv();
        foreach ($data as $row) {
            $csvFile->writeRow($row);
        }
        $options = [
            'delimiter' => $csvFile->getDelimiter(),
            'enclosure' => $csvFile->getEnclosure(),
            'escapedBy' => $csvFile->getEscapedBy(),
        ];

        // upload file
        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['file-import']),
        );
        $options['dataFileId'] = $fileId;
        $this->expectExceptionMessage(
            'Load error: '
            .'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. '
            .'Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; '
            .'line_number: 2 byte_offset_to_start_of_line: 17 col',
        );
        $this->expectException(ClientException::class);
        $this->_client->writeTableAsyncDirect($tableId, $options);
    }

    public function testInsertInvalidValueToTypedColumn(): void
    {
        $bucketId = $this->getTestBucketId();

        $data = [
            'name' => 'test-table',
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
                    'name' => 'price',
                    'definition' => [
                        'type' => 'NUMERIC',
                        'nullable' => false,
                    ],
                ],
            ],
        ];

        $tableId = $this->_client->createTableDefinition($bucketId, $data);

        $workspace = $this->initTestWorkspace('bigquery');

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_prices');

        /** @var BigQueryClient $db */
        $db = $backend->getDb();

        $qb = new BigqueryTableQueryBuilder();
        $db->runQuery($db->query($qb->getCreateTableCommand(
            $workspace['connection']['schema'],
            'test_prices',
            new ColumnCollection([
                new BigqueryColumn('id', new Bigquery('INT64')),
                new BigqueryColumn('price', new Bigquery('STRING')),
            ]),
        )));
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            '
INSERT INTO %s.`test_prices` (`id`, `price`) VALUES (1, \'too expensive\') ;',
            $workspace['connection']['schema'],
        ));

        $this->expectExceptionMessage('Load error: Source destination columns mismatch. "price STRING"->"price NUMERIC"');
        $this->expectException(ClientException::class);
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_prices',
        ]);
    }

    public function testCreateTableDefaults(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $types = [
            'BOOL' => [
                'working bool bool string' => [
                    'value' => 'true',
                    'expectFail' => [],
                ],
                'working bool false string' => [
                    'value' => 'false',
                    'expectFail' => [],
                ],
                'working bool true' => [
                    'value' => true,
                    'expectFail' => [],
                ],
                'working bool false' => [
                    'value' => false,
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'test',
                    'expectFail' => [
                        'message' => 'Boolean default value "test" is not boolean.',
                    ],
                ],
                'fail type 2' => [
                    'value' => 123,
                    'expectFail' => [
                        'message' => 'Boolean default value "123" is not boolean.',
                    ],
                ],
            ],
            'BYTES' => [
                'working' => [
                    'value' => 'B"abc"',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'DATE' => [
                'working' => [
                    'value' => '2022-02-22',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail quoted' => [
                    'value' => '\'2022-02-22\'',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'DATETIME' => [
                'working' => [
                    'value' => 'CURRENT_DATETIME()',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [
                    ],
                ],
                'fail quoted' => [
                    'value' => '\'2021-01-01 00:00:00\'',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
                'datetime' => [
                    'value' => '2021-01-01 00:00:00',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'TIME' => [
                'working' => [
                    'value' => 'CURRENT_TIME()',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [
                    ],
                ],
                'time' => [
                    'value' => '00:00:00',
                    'expectFail' => [],
                ],
                'fail quoted' => [
                    'value' => '\'00:00:00\'',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'TIMESTAMP' => [
                'working' => [
                    'value' => 'current_timestamp()',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [
                    ],
                ],
                'fail quoted' => [
                    'value' => '\'2021-01-01 00:00:00\'',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
                'timestamp' => [
                    'value' => '2021-01-01 00:00:00',
                    'expectFail' => [
                    ],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'GEOGRAPHY' => [
                'working' => [
                    'value' => 'ST_GEOGPOINT(-122.4194, 37.7749)',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'JSON' => [
                'working' => [
                    'value' => 'JSON\'{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}\'',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'INT64' => [
                'working' => [
                    'value' => '1',
                    'expectFail' => [],
                ],
                'working_num' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],

            'NUMERIC' => [
                'working' => [
                    'value' => '1',
                    'expectFail' => [],
                ],
                'working float' => [
                    'value' => '1.23',
                    'expectFail' => [],
                ],
                'working_num' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'BIGNUMERIC' => [
                'working' => [
                    'value' => '1',
                    'expectFail' => [],
                ],
                'working float' => [
                    'value' => '1.23',
                    'expectFail' => [],
                ],
                'working_num' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'FLOAT64' => [
                'working' => [
                    'value' => '1',
                    'expectFail' => [],
                ],
                'working float' => [
                    'value' => '1.23',
                    'expectFail' => [],
                ],
                'working_num' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'fail type' => [
                    'value' => 'string',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
            ],
            'STRING' => [
                'working' => [
                    'value' => '\'T\'',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [],
                ],
                'not quoted' => [
                    'value' => 'test',
                    'expectFail' => [],
                ],
                'quoted' => [
                    'value' => '"test"',
                    'expectFail' => [],
                ],
                'type number' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
            ],
        ];

        foreach ($types as $type => $cases) {
            $columnName = 'c_' . str_replace(' ', '', strtolower($type));
            foreach ($cases as $caseName => $options) {
                $tableName = 'test_' . $columnName . '_' . str_replace(' ', '_', $caseName);
                $tableDefinition = [
                    'name' => $tableName,
                    'primaryKeysNames' => [],
                    'columns' => [
                        [
                            'name' => $columnName,
                            'definition' => [
                                'type' => $type,
                                'default' => $options['value'],
                            ],
                        ],
                    ],
                ];
                $expectFail = array_key_exists('message', $options['expectFail']);
                $expectedMessage = '';
                if ($expectFail) {
                    // @phpstan-ignore-next-line
                    $expectedMessage = $options['expectFail']['message'];
                }
                try {
                    $this->_client->createTableDefinition($bucketId, $tableDefinition);
                    if ($expectFail) {
                        $this->fail(sprintf(
                            'Testing datatype "%s" with case "%s" not failed. Expected exception was: "%s"',
                            $type,
                            $caseName,
                            $expectedMessage,
                        ));
                    }
                } catch (Throwable $e) {
                    if ($e instanceof AssertionFailedError) {
                        throw $e;
                    }
                    if (!$expectFail) {
                        $this->fail(sprintf(
                            'Testing datatype "%s" with case "%s" was not expected to fail. Error is: "%s"',
                            $type,
                            $caseName,
                            $e->getMessage(),
                        ));
                    }
                    $this->assertInstanceOf(ClientException::class, $e);
                    $this->assertStringStartsWith(
                        $expectedMessage,
                        $e->getMessage(),
                        sprintf(
                            'Testing datatype "%s" with case "%s" was not expected exception message: "%s"',
                            $type,
                            $caseName,
                            $e->getMessage(),
                        ),
                    );
                }
            }
        }
    }

    /**
     * this testcase is not executed, because it takes too long for very low value. Code is being kept for future reference
     * @dataProvider provideDataForIllogicalFilter
     */
    public function skipTestIllogicalComparisonInFilter(array $filter): void
    {
        $bucketId = $this->getTestBucketId();

        $columns = [
            [
                'name' => 'id',
                'definition' => [
                    'type' => 'INT64',
                    'nullable' => false,
                ],
            ],
        ];
        $types = $this->providePlainBqTypes();

        foreach ($types as $type) {
            $columns[] = [
                'name' => $type,
                'definition' => [
                    'type' => $type,
                    'nullable' => true,
                ],
            ];
        }

        $data = [
            'name' => 'my_new_table_with_nulls',
            'primaryKeysNames' => ['id'],
            'columns' => $columns,
        ];

        $tableId = $bucketId . '.' . 'my_new_table_with_nulls';
        if (!$this->_client->tableExists($tableId)) {
            $tableId = $this->_client->createTableDefinition($bucketId, $data);
        }

        try {
            $this->_client->getTableDataPreview($tableId, [
                'format' => 'json',
                'whereFilters' => $filter,
            ]);
            $this->expectNotToPerformAssertions();
        } catch (ClientException $e) {
            $this->assertMatchesRegularExpression('/Invalid cast from.*/', $e->getMessage());
        }
    }

    public function provideDataForIllogicalFilter(): Generator
    {
        $operators = [
            'lt',
            'gt',
            'eq',
            'ne',
        ];
        foreach ($this->provideFilterTypes() as $filterType => $valueToTest) {
            foreach ($operators as $operator) {
                foreach ($this->providePlainBqTypes() as $bigqueryType) {
                    $filter = [];
                    $filter[] = [
                        'column' => $bigqueryType,
                        'operator' => $operator,
                        'values' => [$valueToTest],
                        'dataType' => $filterType,
                    ];
                    yield $filterType . ' ' . $operator . ' ' . $valueToTest . ' ' . $bigqueryType => [$filter];
                }
            }
        }
    }

    private function providePlainBqTypes(): array
    {
        return [
//            'ARRAY',
            'BIGNUMERIC',
            'BOOL',
//            'BYTES',
            'DATE',
            'DATETIME',
            'FLOAT64',
//            'GEOGRAPHY',
            'INT64',
//            'INTERVAL',
//            'JSON',
            'NUMERIC',
            'STRING',
//            'STRUCT',
            'TIME',
            'TIMESTAMP',
        ];
    }

    private function provideFilterTypes(): array
    {
        return [
            'INTEGER' => 42,
            'DOUBLE' => 42.1,
            'BIGINT' => 4242424242,
            'REAL' => 42.1,
            'DECIMAL' => 42.3,
        ];
    }

    public function testIllogicalComparisonInFilterWithBool(): void
    {
        $bucketId = $this->getTestBucketId();

        $columns = [
            [
                'name' => 'id',
                'definition' => [
                    'type' => 'INT64',
                    'nullable' => false,
                ],
            ],
        ];
        $types = ['BOOL'];

        foreach ($types as $type) {
            $columns[] = [
                'name' => $type,
                'definition' => [
                    'type' => $type,
                    'nullable' => true,
                ],
            ];
        }

        $data = [
            'name' => 'test_table',
            'primaryKeysNames' => ['id'],
            'columns' => $columns,
        ];

        $tableId = $bucketId . '.' . 'test_table';
        if (!$this->_client->tableExists($tableId)) {
            $tableId = $this->_client->createTableDefinition($bucketId, $data);
        }

        try {
            $this->_client->getTableDataPreview($tableId, [
                'format' => 'json',
                'whereFilters' => [
                    [
                        'column' => 'BOOL',
                        'operator' => 'eq',
                        'values' => [false],
                    ],
                ],
            ]);
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertEquals('Invalid filter value, expected:"BOOL", actual:"STRING".', $e->getMessage());
        }
    }
}
