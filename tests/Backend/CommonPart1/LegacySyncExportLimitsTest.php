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

class LegacySyncExportLimitsTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }


    public function testThereisNoDefaultLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->apiGet("storage/tables/{$tableId}/export");
        $this->assertCount(2000, Client::parseCsv($preview), 'all rows should be returned');
    }

    public function testLegacySyncExportParametrizedLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', $csvFile);

        $preview = $this->_client->getTableDataPreview($tableId, [
            'limit' => 2,
        ]);
        $this->assertCount(2, Client::parseCsv($preview), 'only preview of 2 rows should be returned');
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
