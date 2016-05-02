<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 02/05/16
 * Time: 16:22
 */
namespace Keboola\Test\Backend\Redshift;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class CommonTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testBucketUpdateOnTableTruncate()
    {
        $inBucketId = $this->getTestBucketId(self::STAGE_IN);
        $inBucket = $this->_client->getBucket($inBucketId);

        $this->assertEquals(0, $inBucket['rowsCount']);
        $this->assertEquals(0, $inBucket['dataSizeBytes']);


        $tableId = $this->_client->createTable(
            $inBucketId,
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv')
        );

        $inBucket = $this->_client->getBucket($inBucketId);

        $this->assertEquals(200, $inBucket['rowsCount']);
        $this->assertEquals(98566144, $inBucket['dataSizeBytes']);

        // Truncate the new table
        $this->_client->deleteTableRows($tableId);

        // Get a fresh bucket response, it should have the new stats
        $inBucket = $this->_client->getBucket($inBucketId);

        $this->assertEquals(0, $inBucket['rowsCount']);
        $this->assertEquals(0, $inBucket['dataSizeBytes']);
    }

}