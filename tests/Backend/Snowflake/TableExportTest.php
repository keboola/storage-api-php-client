<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Snowflake;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class TableExportTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSyncExportShouldReturnErrorForLargeNumberOfCols(): void
    {
        $cols = implode(',', array_map(function ($colNum) {
            return "col_{$colNum}";
        }, range(1, 130)));

        // sync create table is deprecated and does not support JSON
        $table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
            'dataString' => $cols,
            'name' => 'langs',
        ]);

        try {
            $this->_client->getTableDataPreview($table['id']);
            $this->fail('Table should not be exported');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.maxNumberOfColumnsExceed', $e->getStringCode());
        }
    }

    public function testSyncExportMax30cols(): void
    {
        $cols = [];
        $rows = 4;
        for ($i = 0; $i < $rows; $i++) {
            $cols[] = implode(',', array_map(function ($colNum) {
                return "data_{$colNum}";
            }, range(1, 30)));
        }

        // sync create table is deprecated and does not support JSON
        $table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
            'dataString' => implode("\n", $cols),
            'name' => 'langs',
        ]);

        $data = $this->_client->getTableDataPreview($table['id']);
        $this->assertEquals($rows, count(explode("\n", trim($data))));
    }
}
