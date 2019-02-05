<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class DataPreviewFilterTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testDataPreviewWithWhereConditions()
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                "column" => "column_string",
                "operator" => "eq",
                "values" => ["first"],
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);

        $this->assertCount(1, Client::parseCsv($preview));
    }

    public function testDataPreviewWithCast()
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_string_number',
                'operator' => 'ge',
                'values' => ['6'],
                'dataType' => 'NUMBER',
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $csv = Client::parseCsv($preview);

        $this->assertCount(1, $csv);
        $this->assertEquals($csv[0]['column_string'], 'fifth');
    }

    public function testDataPreviewWithNonExistingDataType()
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_string_number',
                'operator' => 'ge',
                'values' => ['6'],
                'dataType' => 'non-existing'
            ],
        ];
        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp('~Data type non-existing not recognized~');
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    public function testCastToDouble()
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_double',
                'operator' => 'ge',
                'values' => ['4.123'],
                'dataType' => 'DOUBLE'
            ],
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $csv = Client::parseCsv($preview);

        $this->assertCount(2, $csv);
    }

    public function testMultipleConditions()
    {
        $tableId = $this->prepareTable();

        $where = [
            [
                'column' => 'column_double',
                'operator' => 'ge',
                'values' => ['4.123'],
                'dataType' => 'DOUBLE'
            ],
            [
                'column' => 'column_string_number',
                'operator' => 'lt',
                'values' => ['5'],
                'dataType' => 'NUMBER'
            ]
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $csv = Client::parseCsv($preview);

        $this->assertCount(1, $csv);
    }

    public function testInvalidComparingOperator()
    {
        $tableId = $this->prepareTable();

        $where = [
          [
              'column' => 'column_double',
              'operator' => 'non-existing',
              'values' => [123]
          ]
        ];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp('~Invalid where operator non-existing~');
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    private function prepareTable(): string
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['column_string', 'column_string_number', 'column_double']);
        $csvFile->writeRow(['first', '1', '003.123']);
        $csvFile->writeRow(['second', '4', '3.123']);
        $csvFile->writeRow(['third', '4', '0004.123']);
        $csvFile->writeRow(['fifth', '5', '4']);
        $csvFile->writeRow(['fifth', '555111', '5.1234']);
        return $this->_client->createTable($this->getTestBucketId(), 'conditions', $csvFile);
    }
}
