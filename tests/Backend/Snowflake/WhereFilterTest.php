<?php


namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
use Symfony\Component\Filesystem\Filesystem;
use function GuzzleHttp\json_encode;

class WhereFilterTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSimpleWhereConditions(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_string',
                'operator' => 'eq',
                'values' => ['first'],
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $this->assertCount(1, Client::parseCsv($preview));
        $this->assertCount(1, $this->getExportedTable($tableId, ['whereFilters' => $where]));
    }

    public function testFilterWithCast(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_string_number',
                'operator' => 'ge',
                'values' => ['6'],
                'dataType' => 'INTEGER',
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $previewCsv = Client::parseCsv($preview);
        $exportCsv = $this->getExportedTable($tableId, ['whereFilters' => $where]);

        $this->assertCount(1, $previewCsv);
        $this->assertCount(1, $exportCsv);
        $this->assertEquals($previewCsv[0]['column_string'], 'fifth');
        $this->assertEquals($exportCsv[0]['column_string'], 'fifth');
    }

    public function testDataPreviewWithNonExistingDataType(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_string_number',
                'operator' => 'ge',
                'values' => ['6'],
                'dataType' => 'non-existing',
            ],
        ];
        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('~Data type non-existing not recognized~');
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    public function testTableExportWithNonExistingDataType(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_string_number',
                'operator' => 'ge',
                'values' => ['6'],
                'dataType' => 'non-existing',
            ],
        ];
        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('~Data type non-existing not recognized~');
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testCastToDouble(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_double',
                'operator' => 'ge',
                'values' => ['4.123'],
                'dataType' => 'DOUBLE',
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $previewCsv = Client::parseCsv($preview);
        $exportCsv = $this->getExportedTable($tableId, ['whereFilters' => $where]);

        $this->assertCount(2, $exportCsv);
        $this->assertCount(2, $previewCsv);
    }

    public function testMultipleConditions(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_double',
                'operator' => 'ge',
                'values' => ['4.123'],
                'dataType' => 'DOUBLE',
            ],
            [
                'column' => 'column_string_number',
                'operator' => 'lt',
                'values' => ['5'],
                'dataType' => 'INTEGER',
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $previewCsv = Client::parseCsv($preview);
        $exportCsv = $this->getExportedTable($tableId, ['whereFilters' => $where]);

        $this->assertCount(1, $previewCsv);
        $this->assertCount(1, $exportCsv);
    }

    public function testDataPreviewInvalidComparingOperator(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_double',
                'operator' => 'non-existing',
                'values' => [123],
            ],
        ];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('~Invalid where operator non-existing~');
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    public function testExportTableInvalidComparingOperator(): void
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_double',
                'operator' => 'non-existing',
                'values' => [123],
            ],
        ];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('~Invalid where operator non-existing~');
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testInvalidStructuredQueryInAsyncExport(): void
    {
        $tableId = $this->prepareTable();

        $where = 'string';

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Parameter \"whereFilters\" should be an array, but parameter contains:\n" . json_encode($where));
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testNonArrayParamsShouldReturnErrorInAsyncExport(): void
    {
        $tableId = $this->prepareTable();

        $where = ['column' => 'column'];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("All items in param \"whereFilters\" should be an arrays, but parameter contains:\n" . json_encode($where));
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testInvalidStructuredQueryInDataPreview(): void
    {
        $tableId = $this->prepareTable();

        $where = 'string';

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Parameter \"whereFilters\" should be an array, but parameter contains:\n" . json_encode($where));
        //@phpstan-ignore-next-line
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    public function testNonArrayParamsShouldReturnErrorInDataPreview(): void
    {
        $tableId = $this->prepareTable();

        $where = ['column' => 'column'];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("All items in param \"whereFilters\" should be an arrays, but parameter contains:\n" . json_encode($where));
        //@phpstan-ignore-next-line
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    private function getExportedTable($tableId, $exportOptions)
    {
        $tableExporter = new TableExporter($this->_client);
        $path = tempnam(sys_get_temp_dir(), 'keboola-export');
        $tableExporter->exportTable($tableId, $path, $exportOptions);
        return Client::parseCsv(file_get_contents($path));
    }

    private function prepareTable()
    {
        $csvFile = $this->createTempCsv();
        $csvFile->writeRow(['column_string', 'column_string_number', 'column_double']);
        $csvFile->writeRow(['first', '1', '003.123']);
        $csvFile->writeRow(['second', '4', '3.123']);
        $csvFile->writeRow(['third', '4', '0004.123']);
        $csvFile->writeRow(['fifth', '5', '4']);
        $csvFile->writeRow(['fifth', '555111', '5.1234']);
        return $this->_client->createTableAsync($this->getTestBucketId(), 'conditions', $csvFile);
    }

    public function testDataPreviewOnTypedTableWithWhereFilters(): void
    {
        $bucketId = $this->getTestBucketId();
        $tableDefinition = [
            'name' => 'tryCastTable',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'column_int',
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
            'column_int',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);
        $csvFile->writeRow([
            1,
            1.1, // decimal gets trailing zeros NUMERIC(38,9) -> 1.100000000
            1.5,
            'true',
            '1989-08-31',
            '1989-08-31 00:00:00.000',
            '1.5',
        ]);
        $csvFile->writeRow([
            2,
            2.5,
            2.5,
            'true',
            '1989-08-31',
            '1989-08-31 00:00:00.000',
            '2.5',
        ]);

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        /** @var array $data */
        $data = $this->_client->getTableDataPreview(
            $tableId,
            [
                'format' => 'json',
                'whereFilters' => [
                    // no casting
                    [
                        'column' => 'column_int',
                        'operator' => 'lt',
                        'values' => [2],
                        'dataType' => 'INTEGER',
                    ],
                    [
                        'column' => 'column_decimal',
                        'operator' => 'lt',
                        'values' => [2],
                        'dataType' => 'DOUBLE',
                    ],
                    [
                        'column' => 'column_float',
                        'operator' => 'lt',
                        'values' => [2],
                        'dataType' => 'DOUBLE',
                    ],
                    // known bug: value has to be convertible to boolean: 0,TRUE,T... e.g. 2 will fail
                    [
                        'column' => 'column_boolean',
                        'operator' => 'eq',
                        'values' => ['1'],
                        'dataType' => 'DOUBLE',
                    ],
                    // casting
                    [
                        'column' => 'column_date',
                        'operator' => 'gt',
                        'values' => [0],
                        'dataType' => 'DOUBLE',
                    ],
                    [
                        'column' => 'column_timestamp',
                        'operator' => 'gt',
                        'values' => [0],
                        'dataType' => 'INTEGER',
                    ],
                    [
                        'column' => 'column_varchar',
                        'operator' => 'lt',
                        'values' => [2],
                        'dataType' => 'DOUBLE',
                    ],
                ],
            ],
        );

        $expectedPreview = [
            [
                [
                    'columnName' => 'column_int',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_decimal',
                    'value' => '1.100000000',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_float',
                    'value' => '1.5',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'column_boolean',
                    'value' => 'true',
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
                    'value' => '1.5',
                    'isTruncated' => false,
                ],
            ],
        ];

        $this->assertSame(
            $expectedPreview,
            $data['rows'],
        );
    }

    public function testExportTableOnTypedTableWithWhereFilters(): void
    {
        $bucketId = $this->getTestBucketId();
        $tableDefinition = [
            'name' => 'tryCastTable',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'column_int',
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
            'column_int',
            'column_decimal',
            'column_float',
            'column_boolean',
            'column_date',
            'column_timestamp',
            'column_varchar',
        ]);

        $csvFile->writeRow([
            1,
            1.1, // decimal gets trailing zeros NUMERIC(38,9) -> 1.100000000
            1.5,
            'true',
            '1989-08-31',
            '1989-08-31 00:00:00.000',
            '1.5',
        ]);
        $csvFile->writeRow([
            2,
            2.5,
            2.5,
            'true',
            '1989-08-31',
            '1989-08-31 00:00:00.000',
            '2.5',
        ]);

        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTableAsync($tableId, $csvFile);

        /** @var array $data */
        $data = $this->_client->exportTableAsync(
            $tableId,
            [
                'whereFilters' => [
                    // no casting
                    [
                        'column' => 'column_int',
                        'operator' => 'lt',
                        'values' => ['2'],
                        'dataType' => 'INTEGER',
                    ],
                    [
                        'column' => 'column_decimal',
                        'operator' => 'lt',
                        'values' => ['2'],
                        'dataType' => 'DOUBLE',
                    ],
                    [
                        'column' => 'column_float',
                        'operator' => 'lt',
                        'values' => ['2'],
                        'dataType' => 'DOUBLE',
                    ],
                    // known bug: value has to be convertible to boolean: 0,TRUE,T... e.g. 2 will fail
                    [
                        'column' => 'column_boolean',
                        'operator' => 'eq',
                        'values' => ['1'],
                        'dataType' => 'DOUBLE',
                    ],
                    // casting
                    [
                        'column' => 'column_date',
                        'operator' => 'gt',
                        'values' => ['0'],
                        'dataType' => 'DOUBLE',
                    ],
                    [
                        'column' => 'column_timestamp',
                        'operator' => 'gt',
                        'values' => ['0'],
                        'dataType' => 'INTEGER',
                    ],
                    [
                        'column' => 'column_varchar',
                        'operator' => 'lt',
                        'values' => ['2'],
                        'dataType' => 'DOUBLE',
                    ],
                ],
            ],
        );

        $tmpDestination = __DIR__ . '/../_tmp/testing_file_name';
        if (file_exists($tmpDestination)) {
            $fs = new Filesystem();
            $fs->remove($tmpDestination);
        }

        $slices = $this->_client->downloadSlicedFile($data['file']['id'], $tmpDestination);

        $csv = '';
        foreach ($slices as $slice) {
            $csv .= file_get_contents($slice);
        }
        $parsedData = Client::parseCsv($csv, false, ',', '"');
        $expectedPreview = [
            '1',
            '1.100000000', // decimal gets trailing zeros NUMERIC(38,9) -> 1.100000000
            '1.5',
            'true',
            '1989-08-31',
            '1989-08-31 00:00:00',
            '1.5',
        ];

        $this->assertArrayEqualsSorted($expectedPreview, $parsedData[0], 0);
    }
}
