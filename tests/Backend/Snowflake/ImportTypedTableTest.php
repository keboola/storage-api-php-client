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
            ],
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
        $firstLoad24Row = null;
        $firstLoad26Row = null;
        foreach ($firstLoadData as $row) {
            if ($row['id'] === '0') {
                $firstLoadZeroRow = $row;
                continue;
            }
            if ($row['id'] === '24') {
                $firstLoad24Row = $row;
                continue;
            }
            if ($row['id'] === '26') {
                $firstLoad26Row = $row;
                continue;
            }
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
            ],
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
        // 24, 0 has old timestamp
        // 26 have new timestamp because of changed value
        $secondLoadZeroRow = null;
        $secondLoad24Row = null;
        $secondLoad26Row = null;
        $secondLoadOtherRows = [];
        foreach ($secondLoadData as $row) {
            if ($row['id'] === '0') {
                $secondLoadZeroRow = $row;
                continue;
            }
            if ($row['id'] === '24') {
                $secondLoad24Row = $row;
                continue;
            }
            if ($row['id'] === '26') {
                $secondLoad26Row = $row;
                continue;
            }
            $secondLoadOtherRows[] = $row;
        }
        $this->assertNotNull($secondLoadZeroRow);
        // 0 id row is same including timestamp
        $this->assertSame($firstLoadZeroRow, $secondLoadZeroRow);
        // 24 id row is same including timestamp it was in both increment and full and value did not changed
        $this->assertSame($firstLoad24Row, $secondLoad24Row);
        // 26 id has changed value and timestamp
        $this->assertNotEquals($firstLoad26Row['_timestamp'], $secondLoad26Row['_timestamp']);
        $this->assertNotEquals($firstLoad26Row['name'], $secondLoad26Row['name']);
        // 26 timestamp is same as new rows from second load
        $this->assertEquals($secondLoad26Row['_timestamp'], $secondLoadOtherRows[0]['_timestamp']);
        foreach ($secondLoadOtherRows as $row) {
            // all timestamps of other rows are same
            $this->assertEqualsWithDelta($secondLoadOtherRows[0]['_timestamp'], $row['_timestamp'], 1);
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
            ],
        );
        $table = $this->_client->getTable($tableId);

        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertEquals($csvFile->getHeader(), $table['columns']);

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $this->assertSame(
            $expectedPreview,
            $data['rows'],
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

    public function testImportExoticDatatypes(): void
    {
        $bucketId = $this->getTestBucketId();

        $payload = [
            'name' => 'exotic',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'INTEGER']],
                ['name' => 'VARIANT', 'definition' => ['type' => 'VARIANT']],
                ['name' => 'BINARY', 'definition' => ['type' => 'BINARY']],
                ['name' => 'VARBINARY', 'definition' => ['type' => 'VARBINARY']],
                ['name' => 'OBJECT', 'definition' => ['type' => 'OBJECT']],
                ['name' => 'ARRAY', 'definition' => ['type' => 'ARRAY']],
                ['name' => 'GEOGRAPHY', 'definition' => ['type' => 'GEOGRAPHY']],
                ['name' => 'GEOMETRY', 'definition' => ['type' => 'GEOMETRY']],
            ],
        ];
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        $csvFile = new CsvFile(__DIR__ . '/../../_data/exotic-types-snowflake.csv');
        $this->_client->writeTableAsync(
            $tableId,
            $csvFile,
            [
                'incremental' => false,
            ],
        );
        $table = $this->_client->getTable($tableId);

        $this->assertNotEmpty($table['dataSizeBytes']);
        $this->assertEquals($csvFile->getHeader(), $table['columns']);
        $this->assertExpectedExoticDatatypesPreview($tableId);

        // write same data to table incrementally
        // this is only to check if it works
        $this->_client->writeTableAsync(
            $tableId,
            $csvFile,
            [
                'incremental' => true,
            ],
        );
        $this->assertExpectedExoticDatatypesPreview($tableId);
    }

    private function assertExpectedExoticDatatypesPreview(string $tableId): void
    {
        /** @var array $data */
        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $this->assertSame(
            [
                [
                    [
                        'columnName' => 'id',
                        'value' => '1',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'ARRAY',
                        'value' => '[1,2,3,undefined]',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'VARIANT',
                        'value' => '3.14',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'OBJECT',
                        'value' => '{"age":42,"name":"Jones"}',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'BINARY',
                        'value' => '123ABC',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'VARBINARY',
                        'value' => '123ABC',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'GEOGRAPHY',
                        'value' => 'POINT(-122.35 37.55)',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'GEOMETRY',
                        'value' => 'POLYGON((0 0,10 0,10 10,0 10,0 0))',
                        'isTruncated' => false,
                    ],
                ],
            ],
            $data['rows'],
        );
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
                ],
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertStringStartsWith("Table import error: Load error: An exception occurred while executing a query: Numeric value 'male' is not recognized", $e->getMessage());
        }

        try {
            // try import data with wrong types with full load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => true,
                ],
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertStringStartsWith("Table import error: Load error: An exception occurred while executing a query: Numeric value 'male' is not recognized", $e->getMessage());
        }
    }

    public function testLoadTypedTablesLengthOverflowError(): void
    {
        $fullLoadFile = __DIR__ . '/../../_data/users.csv';

        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $payload = [
            'name' => 'users-types',
            'primaryKeysNames' => ['id'],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'INT', 'nullable' => false]],
                ['name' => 'name', 'definition' => ['type' => 'VARCHAR', 'length' => 1]],
                ['name' => 'city', 'definition' => ['type' => 'VARCHAR']],
                ['name' => 'sex', 'definition' => ['type' => 'VARCHAR']],
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
                ],
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertStringMatchesFormat(<<<EOD
Load error: An exception occurred while executing a query: User character length limit (1) exceeded by string 'martin'
  File '%s.users.csv.gz', line 2, character 6
  Row 1, column ""%s""["name":2]
  If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options, please run 'info loading_data' in a SQL client.
EOD
                , $e->getMessage());
        }

        try {
            // try import data with wrong types with incremental load
            $this->_client->writeTableAsync(
                $tableId,
                new CsvFile($fullLoadFile),
                [
                    'incremental' => true,
                ],
            );
            $this->fail('Should throw ClientException');
        } catch (ClientException $e) {
            self::assertStringMatchesFormat(<<<EOD
Load error: An exception occurred while executing a query: User character length limit (1) exceeded by string 'martin'
  File '%s.users.csv.gz', line 2, character 6
  Row 1, column ""%s""["name":2]
  If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options, please run 'info loading_data' in a SQL client.
EOD
                , $e->getMessage());
        }
    }
}
