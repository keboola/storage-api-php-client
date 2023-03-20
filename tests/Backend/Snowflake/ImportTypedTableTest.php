<?php

namespace Keboola\Test\Backend\Snowflake;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class ImportTypedTableTest extends ParallelWorkspacesTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider importEmptyValuesProvider
     */
    public function testImportEmptyValues(string $loadFile, array $expectedPreview): void
    {
        $bucketId = $this->getTestBucketId();

        $payload = [
            'name' => 'with-empty',
            'primaryKeysNames' => [],
            'columns' => [
                ['name' => 'col', 'definition' => ['type' => 'NUMBER']],
                ['name' => 'str', 'definition' => ['type' => 'VARCHAR']],
            ],
        ];
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        $csvFile = new CsvFile($loadFile);
        $this->_client->writeTableAsync(
            $tableId,
            $csvFile,
            [
                'incremental' => false,
            ]
        );
        $table = $this->_client->getTable($tableId);

        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertEquals($csvFile->getHeader(), $table['columns']);

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows']
        );
    }

    public function importEmptyValuesProvider(): Generator
    {
        yield 'empty values in quotes' => [
            __DIR__ . '/../../_data/empty-with-quotes-numeric.csv',
            [
                [
                    [
                        'columnName' => 'col',
                        'value' => '9',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => '4',
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'col',
                        'value' => '23',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => '4',
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'col',
                        'value' => null,
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => null,
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'col',
                        'value' => '6',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => '6',
                        'isTruncated' => false,
                    ],
                ],
            ],
        ];

        yield 'empty values without quotes' => [
            __DIR__ . '/../../_data/empty-without-quotes-numeric.csv',
            [
                [
                    [
                        'columnName' => 'col',
                        'value' => '2',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => '6',
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'col',
                        'value' => null,
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => null,
                        'isTruncated' => false,
                    ],
                ],
                [
                    [
                        'columnName' => 'col',
                        'value' => '1',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'str',
                        'value' => '3',
                        'isTruncated' => false,
                    ],
                ],
            ],
        ];
    }


    public function testLoadTypedTablesConversionError(): void
    {
        $fullLoadFile = __DIR__ . '/../../_data/users.csv';

        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $payload = [
            'name' => 'users-types',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'INT', 'nullable' => false]],
                ['name' => 'name', 'definition' => ['type' => 'VARCHAR']],
                ['name' => 'city', 'definition' => ['type' => 'VARCHAR']],
                ['name' => 'sex', 'definition' => ['type' => 'INT']],
            ],
        ];
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        // create table
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => false,
                ]
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertSame("Table import error: Load error: An exception occurred while executing a query: Numeric value 'male' is not recognized", $e->getMessage());
        }

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => true,
                ]
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertSame("Table import error: Load error: An exception occurred while executing a query: Numeric value 'male' is not recognized", $e->getMessage());
        }
    }
}
