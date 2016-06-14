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
        $cols = implode(',', array_map(function($colNum) {
            return "col_{$colNum}";
        }, range(1, 40)));

        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => $cols,
            'name' => 'languages',
        ));

        try {
            $this->_client->exportTable($table['id']);
            $this->fail('Table should not be exported');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.maxNumberOfColumnsExceed', $e->getStringCode());
        }
    }

}