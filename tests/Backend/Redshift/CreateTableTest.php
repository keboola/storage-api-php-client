<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Redshift;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class CreateTableTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * @param $async
     * @dataProvider syncAsyncData
     */
    public function testTableWithLongPkShouldNotBeCreatedInRedshift($async): void
    {
        $method = $async ? 'createTableAsync' : 'createTable';
        $id = $this->_client->{$method}(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            [
                'primaryKey' => 'Paid_Search_Engine_Account,Date,Paid_Search_Campaign,Paid_Search_Ad_ID,Site__DFA',
            ]
        );
        $this->assertNotEmpty($id);
    }

    public function testTimeTravelNotSupported(): void
    {
        $id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        try {
            $id = $this->_client->createTableFromSourceTableAtTimestamp(
                $this->getTestBucketId(self::STAGE_OUT),
                $id,
                date(DATE_ATOM),
                'attempted-ts',
            );
            $this->fail('TimeTravel is not supprted in redshift');
        } catch (ClientException $exception) {
            $this->assertEquals('storage.validation.timeTravelNotSupported', $exception->getStringCode());
        }
    }

    public function syncAsyncData()
    {
        return [
            [false],
            [true],
        ];
    }
}
