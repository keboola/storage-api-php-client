<?php



namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class WhereFilterTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testSimpleWhereConditions()
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
        $this->assertCount(1, $this->getExportedTable($tableId, ['whereFilters' => $where]));
    }

    public function testFilterWithCast()
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

    public function testTableExportWithNonExistingDataType()
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
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
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
        $previewCsv = Client::parseCsv($preview);
        $exportCsv = $this->getExportedTable($tableId, ['whereFilters' => $where]);

        $this->assertCount(2, $exportCsv);
        $this->assertCount(2, $previewCsv);
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
                'dataType' => 'INTEGER'
            ]
        ];
        $preview = $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
        $previewCsv = Client::parseCsv($preview);
        $exportCsv = $this->getExportedTable($tableId, ['whereFilters' => $where]);

        $this->assertCount(1, $previewCsv);
        $this->assertCount(1, $exportCsv);
    }

    public function testDataPreviewInvalidComparingOperator()
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

    public function testExportTableInvalidComparingOperator()
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
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testInvalidStructuredQueryInAsyncExport()
    {
        $tableId = $this->prepareTable();

        $where = "string";

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp("/Parameter \"whereFilters\" should be an array, but request contains:/");
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testNonArrayParamsShouldReturnErrorInAsyncExport()
    {
        $tableId = $this->prepareTable();

        $where = ['column' => 'column'];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp("/All items in param \"whereFilters\" should be an arrays, but request contains:/");
        $this->getExportedTable($tableId, ['whereFilters' => $where]);
    }

    public function testInvalidStructuredQueryInDataPreview()
    {
        $tableId = $this->prepareTable();

        $where = "string";

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp("/Parameter \"whereFilters\" should be an array, but request contains:/");
        $this->_client->getTableDataPreview($tableId, ['whereFilters' => $where]);
    }

    public function testNonArrayParamsShouldReturnErrorInDataPreview()
    {
        $tableId = $this->prepareTable();

        $where = ['column' => 'column'];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageRegExp("/All items in param \"whereFilters\" should be an arrays, but request contains:/");
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
