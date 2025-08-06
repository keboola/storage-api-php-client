<?php



namespace Keboola\Test\Backend\Exasol;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
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
                'dataType' => 'DECIMAL',
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
                'dataType' => 'DECIMAL',
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
                'dataType' => 'DECIMAL',
            ],
            [
                'column' => 'column_string_number',
                'operator' => 'lt',
                'values' => ['5'],
                'dataType' => 'DECIMAL',
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
}
