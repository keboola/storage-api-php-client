<?php
/**
 *
 * User: Ondřej Hlaváček
 * Date: 13.8.12
 * Time: 16:40
 *
 */

class Keboola_StorageApi_ConfigsTest extends StorageApiTestCase
{

	protected $_inBucketId;

	public function setUp()
	{
		parent::setUp();
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');
		$this->_client->setBucketAttribute($this->_inBucketId, "property1", "value1");
		$table1Id = $this->_client->createTable($this->_inBucketId, "config1", __DIR__ . '/_data/config.csv');
		$table2Id = $this->_client->createTable($this->_inBucketId, "config2", __DIR__ . '/_data/config.csv');
		$this->_client->setTableAttribute($table1Id, "property2", "value2");
		$this->_client->setTableAttribute($table2Id, "nestedProperty.property", "value3");
		$this->_client->setTableAttribute($table2Id, "nestedProperty2.level2.property", "value3");
	}

	public function testConfig()
	{
		\Keboola\StorageApi\Config\Reader::$client = $this->_client;
		$config = \Keboola\StorageApi\Config\Reader::read($this->_inBucketId);

		$this->assertEquals($config["property1"], "value1", "bucket attribute");
		$this->assertEquals($config["items"]["config1"]["property2"], "value2", "table attribute");
		$this->assertEquals($config["items"]["config2"]["nestedProperty"]["property"], "value3", "nested table attribute");
		$this->assertEquals($config["items"]["config2"]["nestedProperty2"]["level2"]["property"], "value3", "another nested table attribute");
		$this->assertEquals($config["items"]["config1"]["items"][0]["query"], "SELECT * FROM table", "table content");

	}
}