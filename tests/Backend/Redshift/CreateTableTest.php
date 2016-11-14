<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Redshift;

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
    public function testTableWithLongPkShouldNotBeCreatedInRedshift($async)
    {
        $method = $async ? 'createTableAsync' : 'createTable';
        $id = $this->_client->{$method}(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            array(
                'primaryKey' => 'Paid_Search_Engine_Account,Date,Paid_Search_Campaign,Paid_Search_Ad_ID,Site__DFA',
            )
        );
        $this->assertNotEmpty($id);
    }

    public function syncAsyncData()
    {
        return array(
            array(false),
            array(true),
        );
    }
}
