<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mysql;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class CreateTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * @param $async
     * @dataProvider syncAsyncData
     */
    public function testTableWithLongPkShouldNotBeCreatedInMysql($async)
    {
        try {
            $method = $async ? 'createTableAsync' : 'createTable';
            $this->_client->{$method}(
                $this->getTestBucketId(self::STAGE_IN),
                'languages',
                new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
                array(
                    'primaryKey' => 'Paid_Search_Engine_Account,Date,Paid_Search_Campaign,Paid_Search_Ad_ID,Site__DFA',
                )
            );
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.primaryKeyTooLong', $e->getStringCode());
        }
    }

    public function syncAsyncData()
    {
        return array(
            array(false),
            array(true),
        );
    }
}
