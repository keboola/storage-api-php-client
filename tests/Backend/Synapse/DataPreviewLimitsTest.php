<?php
namespace Keboola\Test\Backend\Synapse;

use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\CommonPart1\DataPreviewLimitsTest as CommonDataPreviewLimitsTest;

class DataPreviewLimitsTest extends CommonDataPreviewLimitsTest
{
    public function testDataPreviewDefaultLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->getTableDataPreview($tableId);
        $this->assertCount(100, Client::parseCsv($preview), 'only preview of 100 rows should be returned');

//        // @TODO table export is not implementd yet
//        $tableExporter = new TableExporter($this->_client);
//
//        $fullTableExportPath = tempnam(sys_get_temp_dir(), 'keboola');
//        $tableExporter->exportTable($tableId, $fullTableExportPath, []);
//        $this->assertCount(2000, Client::parseCsv(file_get_contents($fullTableExportPath)));
    }

    public function testJsonTruncationLimit()
    {
        $this->markTestSkipped('Columns with large length for Synapse backend is not supported yet');
    }

    private function generateCsv($rowsCount, $collsCount = 2)
    {
        $importFilePath = tempnam(sys_get_temp_dir(), 'keboola');
        $csvFile = new CsvFile($importFilePath);
        $header = [];
        for ($i = 0; $i < $collsCount; $i++) {
            array_push($header, 'col' . $i);
        }
        $csvFile->writeRow($header);
        for ($i = 0; $i < $rowsCount; $i++) {
            $row = [];
            for ($j = 0; $j < $collsCount; $j++) {
                array_push($row, rand());
            }
            $csvFile->writeRow($row);
        }
        return $csvFile;
    }
}
