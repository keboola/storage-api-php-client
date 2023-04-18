<?php

namespace Keboola\Test\Backend\Snowflake;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
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
     * test _timestamp changing in typed tables
     */
    public function testImportDeduplication(): void
    {
        $bucketId = $this->getTestBucketId();
        $payload = [
            'name' => 'dedup',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'basetype' => 'INTEGER'],
                ['name' => 'name', 'basetype' => 'STRING'],
            ],
        ];
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // import table with duplicates
        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.duplicates.csv');
        $this->_client->writeTableAsync(
            $tableId,
            $csvFile,
            [
                'incremental' => false,
            ]
        );
        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
            ],
        ]);
        $firstLoadData = $backend->fetchAll('languages', \PDO::FETCH_ASSOC);
        // there are 3 rows 0,24,26
        $this->assertCount(3, $firstLoadData);
        $firstLoadZeroRow = null;
        $firstLoadOtherRows = [];
        foreach ($firstLoadData as $row) {
            if ($row['id'] === '0') {
                $firstLoadZeroRow = $row;
                continue;
            }
            $firstLoadOtherRows[] = $row;
        }
        $this->assertNotNull($firstLoadZeroRow);

        // import same data second time incrementally
        // file
        // - is missing row id 0
        // - has new value for id 26
        // - has same value for id 24
        // - has new ids 1,11,25
        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.more-rows_without_zero.csv');
        $this->_client->writeTableAsync(
            $tableId,
            $csvFile,
            [
                'incremental' => true,
            ]
        );
        // without preserve workspace is cleaned
        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
            ],
        ]);
        $secondLoadData = $backend->fetchAll('languages', \PDO::FETCH_ASSOC);
        // there are 3 new rows 1,11,25
        $this->assertCount(6, $secondLoadData);
        // there are 3 old rows 24,26 and 0 (0 is also not in incremental load)
        // 24 and 26 have new timestamps
        // 0 has old timestamp
        $secondLoadZeroRow = null;
        $secondLoadOtherRows = [];
        foreach ($secondLoadData as $row) {
            if ($row['id'] === '0') {
                $secondLoadZeroRow = $row;
                continue;
            }
            $secondLoadOtherRows[] = $row;
        }
        $this->assertNotNull($secondLoadZeroRow);
        // 0 id row is same including timestamp
        $this->assertSame($firstLoadZeroRow, $secondLoadZeroRow);
        foreach ($secondLoadOtherRows as $row) {
            // compare timestamps in all rows to be same as first row
            // add delta just for case that all are not imported in same second
            $this->assertEqualsWithDelta($secondLoadOtherRows[0]['_timestamp'], $row['_timestamp'], 1);
            // compare timestamps in all rows not to be same as first row of first load
            // this would not be true if table was not typed and timestamp would be update only for columns with changed values
            // in this case that would be 26 czech->magyar
            $this->assertNotEquals($firstLoadOtherRows[0]['_timestamp'], $row['_timestamp']);
        }
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
