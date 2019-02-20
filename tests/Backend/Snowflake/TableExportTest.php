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
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testSyncExportShouldReturnErrorForLargeNumberOfCols()
    {
        $cols = implode(',', array_map(function ($colNum) {
            return "col_{$colNum}";
        }, range(1, 130)));

        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => $cols,
            'name' => 'langs',
        ));

        try {
            $this->_client->getTableDataPreview($table['id']);
            $this->fail('Table should not be exported');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.maxNumberOfColumnsExceed', $e->getStringCode());
        }
    }

    public function testSyncExportMax30cols()
    {
        $cols = [];
        $rows = 4;
        for ($i = 0; $i < $rows; $i++) {
            $cols[] = implode(',', array_map(function ($colNum) {
                return "data_{$colNum}";
            }, range(1, 30)));
        }

        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => implode("\n", $cols),
            'name' => 'langs',
        ));

        $data = $this->_client->getTableDataPreview($table['id']);
        $this->assertEquals($rows, count(explode("\n", trim($data))));
    }
}
