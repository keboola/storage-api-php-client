<?php
/**
 *
 * User: Ondřej Hlaváček
 * Date: 13.8.12
 * Time: 16:40
 *
 */


namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

class ConfigsTest extends StorageApiTestCase
{

    protected $_inBucketId;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testConfig()
    {
        $inIdBucket = $this->getTestBucketId(self::STAGE_IN);
        $this->_client->setBucketAttribute($inIdBucket, "property1", "value1");
        $table1Id = $this->_client->createTable($inIdBucket, "config1", new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/config.csv'));
        $table2Id = $this->_client->createTable($inIdBucket, "config2", new \Keboola\Csv\CsvFile(__DIR__ . '/../_data/config.csv'));
        $this->_client->setTableAttribute($table1Id, "property2", "value2");
        $this->_client->setTableAttribute($table2Id, "nestedProperty.property", "value3");
        $this->_client->setTableAttribute($table2Id, "nestedProperty2.level2.property", "value3");
        \Keboola\StorageApi\Config\Reader::$client = $this->_client;
        $config = \Keboola\StorageApi\Config\Reader::read($inIdBucket);

        $this->assertEquals($config["property1"], "value1", "bucket attribute");
        $this->assertEquals($config["items"]["config1"]["property2"], "value2", "table attribute");
        $this->assertEquals($config["items"]["config2"]["nestedProperty"]["property"], "value3", "nested table attribute");
        $this->assertEquals($config["items"]["config2"]["nestedProperty2"]["level2"]["property"], "value3", "another nested table attribute");
        $this->assertEquals($config["items"]["config1"]["items"][0]["query"], "SELECT * FROM table", "table content");

        $this->_client->deleteBucketAttribute($inIdBucket, "property1");
    }
}
