<?php

namespace Keboola\Test\Backend\Snowflake;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\StorageApiTestCase;
use PHPUnit\Framework\AssertionFailedError;
use Throwable;

class TableDefinitionOperationsTest extends ParallelWorkspacesTestCase
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
            'name' => 'my_new_table',
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
                        'type' => 'VARCHAR',
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
                        'type' => 'NUMBER',
                        'nullable' => false,
                        'length' => '38,0',
                    ],
                    'basetype' => 'NUMERIC',
                    'canBeFiltered' => true,
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'VARCHAR',
                        'nullable' => true,
                        'length' => '16777216',
                    ],
                    'basetype' => 'STRING',
                    'canBeFiltered' => true,
                ],
            ],
        ], $tableDetail['definition']);
    }

    public function testCreateTableDefaults(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $types = [
            'NUMBER' => [
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
            'DECIMAL' => [
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
            'INT' => [
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
            'INTEGER' => [
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
            'BIGINT' => [
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
            'SMALLINT' => [
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
            'TINYINT' => [
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
            'BYTEINT' => [
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
            'FLOAT' => [
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
            'FLOAT4' => [
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
            'FLOAT8' => [
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
            'DOUBLE' => [
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
            'DOUBLE PRECISION' => [
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
            'REAL' => [
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
            'VARCHAR' => [
                'working' => [
                    'value' => '\'test\'',
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
                'wrong quoted' => [
                    'value' => '"test"',
                    'expectFail' => [],
                ],
                'type number' => [
                    'value' => 123,
                    'expectFail' => [],
                ],
            ],
            'CHAR' => [
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
                'wrong quoted' => [
                    'value' => '"test"',
                    'expectFail' => [],
                ],
                'type number' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
            ],
            'CHARACTER' => [
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
                'wrong quoted' => [
                    'value' => '"test"',
                    'expectFail' => [],
                ],
                'type number' => [
                    'value' => 1,
                    'expectFail' => [],
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
                'wrong quoted' => [
                    'value' => '"test"',
                    'expectFail' => [],
                ],
                'type number' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
            ],
            'TEXT' => [
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
                'wrong quoted' => [
                    'value' => '"test"',
                    'expectFail' => [],
                ],
                'type number' => [
                    'value' => 1,
                    'expectFail' => [],
                ],
            ],
            'BOOLEAN' => [
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
                    'value' => 'current_time()',
                    'expectFail' => [],
                ],
                'empty' => [
                    'value' => '',
                    'expectFail' => [
                    ],
                ],
                'time' => [
                    'value' => '00:00:00',
                    'expectFail' => [
                    ],
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
            'TIMESTAMP_NTZ' => [
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
            'TIMESTAMP_LTZ' => [
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
            'TIMESTAMP_TZ' => [
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
                    'value' => '\'2021-01-01 00:00:00 +0000\'',
                    'expectFail' => [
                        'message' => 'Table creation ended with a syntax exception, probably due to an invalid "default" column specification. Original exception is:',
                    ],
                ],
                'timestamp' => [
                    'value' => '2021-01-01 00:00:00 +0000',
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
                            $expectedMessage
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
                            $e->getMessage()
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
                            $e->getMessage()
                        )
                    );
                }
            }
        }
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

        // remove PK
        $this->_client->removeTablePrimaryKey($tableId);
        // set PK on two columns
        $this->_client->createTablePrimaryKey($tableId, ['id', 'name']);
        $this->assertTableColumnNullable($m, $tableId, 'id', false);
        // second column nullability has not changed SNFLK allows nullable columns in primary key
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
                [
                    'name' => 'column_float',
                    'definition' => [
                        'type' => 'FLOAT',
                    ],
                ],
                [
                    'name' => 'column_boolean',
                    'definition' => [
                        'type' => 'BOOLEAN',
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
            ],
        ];

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $csvHeader = [
            'id',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ];
        // test import value which will not fit
        $csvFile = $this->createTempCsv();
        $csvFile->writeRow($csvHeader);
        $csvFile->writeRow(
            [
                '1',
                '123132456.1264654',
                '3.14',
                0,
                '1989-08-31',
                '1989-08-31 00:00:00.000',
                'roman',
            ]
        );
        try {
            $this->_client->writeTableAsync($tableId, $csvFile);
            $this->fail('Importing value which will not fit into DECIMAL(4,3) should fail');
        } catch (ClientException $e) {
            $this->assertEquals(
                'rowTooLarge',
                $e->getStringCode()
            );
            $this->assertEquals(
                'Load error: An exception occurred while executing a query: Numeric value \'123132456.1264654\' is out of range',
                $e->getMessage()
            );
        }

        $csvFile = $this->createTempCsv();
        $csvFile->writeRow($csvHeader);
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

    public function dataPreviewFiltersProvider(): Generator
    {
        yield 'numeric types' => [
            'columns' => [
                'NUMBER' => [
                    'value' => '1',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '1'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '1'],
                        'lt' => ['count' => 1, 'value' => '2'],
                        'le' => ['count' => 1, 'value' => '1'],
                    ],
                ],
                'DECIMAL' => [
                    'value' => '1',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '1'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '1'],
                        'lt' => ['count' => 1, 'value' => '2'],
                        'le' => ['count' => 1, 'value' => '1'],
                    ],
                ],
                'NUMERIC' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'INT' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'INTEGER' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'BIGINT' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'SMALLINT' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'TINYINT' => [
                    'value' => '1',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '1'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '1'],
                        'lt' => ['count' => 1, 'value' => '2'],
                        'le' => ['count' => 1, 'value' => '1'],
                    ],
                ],
                'BYTEINT' => [
                    'value' => '1',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '1'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '1'],
                        'lt' => ['count' => 1, 'value' => '2'],
                        'le' => ['count' => 1, 'value' => '1'],
                    ],
                ],
                'FLOAT' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'FLOAT4' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'FLOAT8' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'DOUBLE' => [
                    'value' => '123.123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123.123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123.123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123.123'],
                    ],
                ],
                'DOUBLE PRECISION' => [
                    'value' => '123.123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123.123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123.123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123.123'],
                    ],
                ],
                'REAL' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
            ],
            'expectedPreview' => [
                [
                    'columnName' => 'column_number',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_decimal',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_numeric',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_int',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_integer',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_bigint',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_smallint',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_tinyint',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_byteint',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float4',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float8',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_double',
                    'value' => '123.123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_doubleprecision',
                    'value' => '123.123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_real',
                    'value' => '123',
                    'isTruncated' => false,
                ],
            ],
        ];

        yield 'string types' => [
            'columns' => [
                'VARCHAR' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'CHAR' => [
                    'value' => '1',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '1'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '1'],
                        'lt' => ['count' => 1, 'value' => '2'],
                        'le' => ['count' => 1, 'value' => '1'],
                    ],
                ],
                'CHARACTER' => [
                    'value' => '1',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '1'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '1'],
                        'lt' => ['count' => 1, 'value' => '2'],
                        'le' => ['count' => 1, 'value' => '1'],
                    ],
                ],
                'STRING' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
                'TEXT' => [
                    'value' => '123',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '123'],
                        'ne' => ['count' => 1, 'value' => '2'],
                        'gt' => ['count' => 1, 'value' => '0'],
                        'ge' => ['count' => 1, 'value' => '123'],
                        'lt' => ['count' => 1, 'value' => '1234'],
                        'le' => ['count' => 1, 'value' => '123'],
                    ],
                ],
            ],
            'expectedPreview' => [
                [
                    'columnName' => 'column_varchar',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_char',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_character',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_string',
                    'value' => '123',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_text',
                    'value' => '123',
                    'isTruncated' => false,
                ],
            ],
        ];

        yield 'bool types' => [
            'columns' => [
                'BOOLEAN' => [
                    'value' => 'TRUE',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => 'true'],
                        'ne' => ['count' => 1, 'value' => 'false'],
                        'gt' => ['count' => 0, 'value' => 'true'],
                        'ge' => ['count' => 1, 'value' => 'true'],
                        'lt' => ['count' => 0, 'value' => 'true'],
                        'le' => ['count' => 1, 'value' => 'true'],
                    ],
                ],
            ],
            'expectedPreview' => [
                [
                    'columnName' => 'column_boolean',
                    'value' => 'true',
                    'isTruncated' => false,
                ],
            ],
        ];

        yield 'date types' => [
            'columns' => [
                'DATE' => [
                    'value' => '2022-02-22',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '2022-02-22'],
                        'ne' => ['count' => 1, 'value' => '2022-02-23'],
                        'gt' => ['count' => 1, 'value' => '2022-02-21'],
                        'ge' => ['count' => 1, 'value' => '2022-02-22'],
                        'lt' => ['count' => 1, 'value' => '2022-02-23'],
                        'le' => ['count' => 1, 'value' => '2022-02-22'],
                    ],
                ],
                'DATETIME' => [
                    'value' => '2021-01-01 10:00:00',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'ne' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'gt' => ['count' => 1, 'value' => '2021-01-01 09:00:00'],
                        'ge' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'lt' => ['count' => 1, 'value' => '2022-01-01 11:00:00'],
                        'le' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                    ],
                ],
                'TIME' => [
                    'value' => '10:00:00',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '10:00:00'],
                        'ne' => ['count' => 1, 'value' => '11:00:00'],
                        'gt' => ['count' => 1, 'value' => '09:00:00'],
                        'ge' => ['count' => 1, 'value' => '10:00:00'],
                        'lt' => ['count' => 1, 'value' => '11:00:00'],
                        'le' => ['count' => 1, 'value' => '10:00:00'],
                    ],
                ],
                'TIMESTAMP' => [
                    'value' => '2021-01-01 10:00:00',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'ne' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'gt' => ['count' => 1, 'value' => '2021-01-01 09:00:00'],
                        'ge' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'lt' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'le' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                    ],
                ],
                'TIMESTAMP_NTZ' => [
                    'value' => '2021-01-01 10:00:00',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'ne' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'gt' => ['count' => 1, 'value' => '2021-01-01 09:00:00'],
                        'ge' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'lt' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'le' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                    ],
                ],
                'TIMESTAMP_LTZ' => [
                    'value' => '2021-01-01 10:00:00',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'ne' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'gt' => ['count' => 1, 'value' => '2021-01-01 09:00:00'],
                        'ge' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                        'lt' => ['count' => 1, 'value' => '2021-01-01 11:00:00'],
                        'le' => ['count' => 1, 'value' => '2021-01-01 10:00:00'],
                    ],
                ],
                'TIMESTAMP_TZ' => [
                    'value' => '2021-01-01 10:00:00 +0000',
                    'compare' => [
                        'eq' => ['count' => 1, 'value' => '2021-01-01 10:00:00 +0000'],
                        'ne' => ['count' => 1, 'value' => '2021-01-01 11:00:00 +0000'],
                        'gt' => ['count' => 1, 'value' => '2021-01-01 09:00:00 +0000'],
                        'ge' => ['count' => 1, 'value' => '2021-01-01 10:00:00 +0000'],
                        'lt' => ['count' => 1, 'value' => '2021-01-01 11:00:00 +0000'],
                        'le' => ['count' => 1, 'value' => '2021-01-01 10:00:00 +0000'],
                    ],
                ],
            ],
            'expectedPreview' => [
                [
                    'columnName' => 'column_date',
                    'value' => '2022-02-22',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_datetime',
                    'value' => '2021-01-01 10:00:00.000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_time',
                    'value' => '10:00:00',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp',
                    'value' => '2021-01-01 10:00:00.000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp_ntz',
                    'value' => '2021-01-01 10:00:00.000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp_ltz',
                    'value' => '2021-01-01 10:00:00.000 -0800',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_timestamp_tz',
                    'value' => '2021-01-01 10:00:00.000 Z',
                    'isTruncated' => false,
                ],
            ],
        ];
    }

    /**
     * @dataProvider dataPreviewFiltersProvider
     * @param array<string,array{
     *     value: string|int,
     *     compare: array<string, array{
     *      count:int,
     *      value:string
     *     }>
     * }> $usedColumns
     * @return void
     * @throws \Keboola\Csv\Exception
     */
    public function testDataPreviewForTableDefinitionWithFilters(array $usedColumns, array $expectedPreview): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $whereFilters = [];
        $values = [];
        $columns = [];
        foreach ($usedColumns as $type => $options) {
            $columnName = 'column_' . str_replace(' ', '', strtolower($type));
            $values[$columnName] = $options['value'];
            $columns[] = [
                'name' => $columnName,
                'definition' => [
                    'type' => $type,
                ],
            ];
            foreach ($options['compare'] as $operator => $expectation) {
                $whereFilters[] = [
                    'expectedCount' => $expectation['count'],
                    'column' => $columnName,
                    'operator' => $operator,
                    'values' => [$expectation['value']],
                ];
            }
        }

        $tableDefinition = [
            'name' => 'testDataPreviewForTableDefinitionWithFilters',
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

        $this->assertSame(
            [$expectedPreview],
            $data['rows']
        );

        // test filters
        foreach ($whereFilters as $filter) {
            $expectedCount = $filter['expectedCount'];
            unset($filter['expectedCount']);
            /** @var array $data */
            $data = $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'json',
                    'whereFilters' => [$filter],
                ]
            );

            $this->assertCount($expectedCount, $data['rows'], sprintf('Filter for column %s failed.', $filter['column']));
        }
    }

    public function testDataPreviewExoticTypes(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = $this->getTableDefinitionExoticDatatypes();

        $workspace = $this->initTestWorkspaceAndLoadTestdata();

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $table = $this->_client->getTable($tableId);
        $this->assertSame([
            [
                'name' => 'array',
                'definition' => [
                    'type' => 'ARRAY',
                    'nullable' => true,
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => false,
            ],
            [
                'name' => 'binary',
                'definition' => [
                    'type' => 'BINARY',
                    'nullable' => true,
                    'length' => '8388608',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => false,
            ],
            [
                'name' => 'geography',
                'definition' => [
                    'type' => 'GEOGRAPHY',
                    'nullable' => true,
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => false,
            ],
            [
                'name' => 'geometry',
                'definition' => [
                    'type' => 'GEOMETRY',
                    'nullable' => true,
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => false,
            ],
            [
                'name' => 'id',
                'definition' => [
                    'type' => 'NUMBER',
                    'nullable' => false,
                    'length' => '38,0',
                ],
                'basetype' => 'NUMERIC',
                'canBeFiltered' => true,
            ],
            [
                'name' => 'object',
                'definition' => [
                    'type' => 'OBJECT',
                    'nullable' => true,
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => false,
            ],
            [
                'name' => 'variant',
                'definition' => [
                    'type' => 'VARIANT',
                    'nullable' => true,
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => false,
            ],
        ], $table['definition']['columns']);

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_exotic_datatypes',
        ]);

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $this->assertSame(
            $this->getExpectedExoticDataPreview(),
            $data['rows']
        );

        $this->assertCount(1, $data['rows']);

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
                    ]
                );
                // fail
            } catch (ClientException $e) {
                $this->assertSame(400, $e->getCode());
                $this->assertSame('storage.backend.exception', $e->getStringCode());
                $this->assertSame(
                    sprintf(
                        'Filtering by column "%s" of type "%s" is not supported by the backend "Snowflake".',
                        $col['name'],
                        $col['definition']['type']
                    ),
                    $e->getMessage()
                );
            }
        }
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
                    'value' => '0', // default is NUMBER(38,0) => no scale => 0
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
                    'type' => 'FLOAT',
                ],
                'basetype' => null,
            ],
            [
                'name' => 'column_boolean',
                'definition' => [
                    'type' => 'BOOLEAN',
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
                'value' => 'FLOAT',
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
            'value' => 'NUMBER',
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
            'key' => 'KBC.datatype.length',
            'value' => '38,0',
            'provider' => 'storage',
        ], $idColumnMetadata[3], ['id', 'timestamp']);

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
            'value' => '16777216',
            'provider' => 'storage',
        ], $nameColumnMetadata[3], ['id', 'timestamp']);

        // check that the new table has datatype metadata same as the table before
        for ($i = 0; $i <= 3; $i++) {
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
     * @dataProvider  filterProvider
     */
    public function testColumnTypesInTableDefinition(array $params, string $expectExceptionMessage): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

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
                    'name' => 'column_datetime',
                    'definition' => [
                        'type' => 'DATETIME',
                    ],
                ],
                [
                    'name' => 'column_date',
                    'definition' => [
                        'type' => 'DATE',
                    ],
                ],
                [
                    'name' => 'column_time',
                    'definition' => [
                        'type' => 'TIME',
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
            'column_datetime',
            'column_date',
            'column_time',
            'column_timestamp',
            'column_boolean',
        ]);
        $csvFile->writeRow(
            [
                '1',
                '3.14',
                '3.14',
                'roman',
                '1989-08-31 00:00:00.000',
                '1989-08-31',
                '12:00:00.000',
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
                'Numeric value \'999999999999999999999999999999999999999\' is not recognized',
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
                'Numeric value \'aaa\' is not recognized',
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
                'Numeric value \'aaa\' is not recognized',
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
                'Numeric value \'aaa\' is not recognized',
            ];

            yield 'wrong datetime '. $format => [
                [
                    'format' => $format,
                    'whereFilters' => [
                        [
                            'column' => 'column_datetime',
                            'operator' => 'eq',
                            'values' => ['2022-02-31'],
                        ],
                    ],
                ],
                'Timestamp \'2022-02-31\' is not recognized', // non-existing date
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
                'Boolean value \'222\' is not recognized',
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
                'Date \'12:00:00.000\' is not recognized',
            ];

            yield 'wrong time '. $format => [
                [
                    'whereFilters' => [
                        [
                            'column' => 'column_time',
                            'operator' => 'eq',
                            'values' => ['1989-08-31'],
                        ],
                    ],
                ],
                'Time \'1989-08-31\' is not recognized',
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
                'Timestamp \'xxx\' is not recognized',
            ];
        }
    }

    private function getExpectedExoticDataPreview(): array
    {
        return [
            [
                [
                    'columnName' => 'id',
                    'value' => '2',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'array',
                    'value' => '[1,2,3,undefined]',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'variant',
                    'value' => '3.14',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'object',
                    'value' => '{"age":42,"name":"Jones"}',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'binary',
                    'value' => '123ABC',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'geography',
                    'value' => 'POINT(-122.35 37.55)',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'geometry',
                    'value' => 'POLYGON((0 0,10 0,10 10,0 10,0 0))',
                    'isTruncated' => false,
                ],
            ],
        ];
    }

    private function getTableDefinitionExoticDatatypes(): array
    {
        return [
            'name' => 'my-new-table-for_data_preview',
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
                    'name' => 'array',
                    'definition' => [
                        'type' => 'ARRAY',
                    ],
                ],
                [
                    'name' => 'variant',
                    'definition' => [
                        'type' => 'VARIANT',
                    ],
                ],
                [
                    'name' => 'object',
                    'definition' => [
                        'type' => 'OBJECT',
                    ],
                ],
                [
                    'name' => 'binary',
                    'definition' => [
                        'type' => 'BINARY',
                    ],
                ],
                [
                    'name' => 'geography',
                    'definition' => [
                        'type' => 'GEOGRAPHY',
                    ],
                ],
                [
                    'name' => 'geometry',
                    'definition' => [
                        'type' => 'GEOMETRY',
                    ],
                ],
            ],
        ];
    }

    private function initTestWorkspaceAndLoadTestdata(): array
    {
        $workspace = $this->initTestWorkspace('snowflake');

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_exotic_datatypes');

        /** @var Connection $db */
        $db = $backend->getDb();

        $qb = new SnowflakeTableQueryBuilder();
        $query = $qb->getCreateTableCommand(
            $workspace['connection']['schema'],
            'test_exotic_datatypes',
            new ColumnCollection([
                new SnowflakeColumn('id', new Snowflake('INT')),
                new SnowflakeColumn('array', new Snowflake('ARRAY')),
                new SnowflakeColumn('variant', new Snowflake('VARIANT')),
                new SnowflakeColumn('object', new Snowflake('OBJECT')),
                new SnowflakeColumn('binary', new Snowflake('BINARY')),
                new SnowflakeColumn('geography', new Snowflake('GEOGRAPHY')),
                new SnowflakeColumn('geometry', new Snowflake('GEOMETRY')),
            ])
        );
        $db->query($query);
        $backend->executeQuery(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."test_exotic_datatypes" ("id", "array", "variant", "object", "binary", "geography", "geometry") 
select 2, ARRAY_CONSTRUCT(1, 2, 3, NULL), TO_VARIANT(\'3.14\'), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT), TO_CHAR(\'123abc\'), \'POINT(-122.35 37.55)\', \'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))\'; 
;',
            $workspace['connection']['schema']
        ));
        return $workspace;
    }

    public function testEmptyZeroLength(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table-for_data_preview',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'columnWithZeroLength',
                    'definition' => [
                        'type' => 'TIMESTAMP_NTZ',
                        'length' => 0,
                        'nullable' => true,
                    ],
                ],
            ],
        ];

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $m = new Metadata($this->_client);
        $columnMetadata = $m->listColumnMetadata("{$tableId}.columnWithZeroLength");

        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.type',
            'value' => 'TIMESTAMP_NTZ',
        ], $columnMetadata[0], ['id', 'timestamp', 'provider']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.nullable',
            'value' => '1',
        ], $columnMetadata[1], ['id', 'timestamp', 'provider']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.basetype',
            'value' => 'TIMESTAMP',
        ], $columnMetadata[2], ['id', 'timestamp', 'provider']);
        $this->assertArrayEqualsExceptKeys([
            'key' => 'KBC.datatype.length',
            'value' => '0',
        ], $columnMetadata[3], ['id', 'timestamp', 'provider']);
    }

}
