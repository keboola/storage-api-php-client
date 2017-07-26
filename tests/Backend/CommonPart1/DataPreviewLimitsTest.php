<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

class DataPreviewLimitsTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }


    public function testDataPreviewDefaultLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->getTableDataPreview($tableId);
        $this->assertCount(100, Client::parseCsv($preview), 'only preview of 100 rows should be returned');

        $tableExporter = new TableExporter($this->_client);

        $fullTableExportPath = tempnam(sys_get_temp_dir(), 'keboola');
        $tableExporter->exportTable($tableId, $fullTableExportPath, []);
        $this->assertCount(2000, Client::parseCsv(file_get_contents($fullTableExportPath)));
    }

    public function testDataPreviewParametrizedLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->getTableDataPreview($tableId, [
            'limit' => 2,
        ]);
        $this->assertCount(2, Client::parseCsv($preview), 'only preview of 2 rows should be returned');
    }

    public function testDataPreviewMaximumLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        try {
            $this->_client->getTableDataPreview($tableId, [
                'limit' => 1200,
            ]);
            $this->fail('limit 1200 should not be allowed');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
            $this->assertContains('1000', $e->getMessage());
        }
    }

    /**
     * @param $rowsCount
     */
    private function generateCsv($rowsCount)
    {
        $importFilePath = tempnam(sys_get_temp_dir(), 'keboola');
        $csvFile = new CsvFile($importFilePath);
        $csvFile->writeRow(['col1', 'col2']);
        for ($i = 0; $i < $rowsCount; $i++) {
            $csvFile->writeRow([
                rand(),
                rand()
            ]);
        }
        return $csvFile;
    }
}
