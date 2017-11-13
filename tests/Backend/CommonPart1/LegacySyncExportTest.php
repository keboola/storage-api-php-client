<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

class LegacySyncExportTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets([self::STAGE_SYS, self::STAGE_IN, self::STAGE_OUT]);
    }

    /**
     * @dataProvider notAllowedStages
     * @param $stage
     */
    public function testIsNotAllowedForStages($stage)
    {
        $csvFile = $this->generateCsv(100);
        $tableId = $this->_client->createTable($this->getTestBucketId($stage), 'users', $csvFile);

        try {
            $this->_client->apiGet("storage/tables/{$tableId}/export");
            $this->fail('Export should not be allowed.');
        } catch (ClientException $e) {
            $this->assertEquals(501, $e->getCode());
        }
    }

    public function notAllowedStages()
    {
        return [
            [
                self::STAGE_IN,
            ],
            [
                self::STAGE_OUT,
            ]
        ];
    }

    public function testThereisNoDefaultLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_SYS), 'users', $csvFile);

        $preview = $this->_client->apiGet("storage/tables/{$tableId}/export");
        $this->assertCount(2000, Client::parseCsv($preview), 'all rows should be returned');
    }

    public function testLegacySyncExportParametrizedLimit()
    {
        $csvFile = $this->generateCsv(2000);
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_SYS), 'users', $csvFile);

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
