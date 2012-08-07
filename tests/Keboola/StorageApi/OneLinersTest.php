<?php
/**
 *
 * User: Ondřej Hlaváček
 * Date: 7.8.12
 * Time: 16:40
 *
 */

class Keboola_StorageApi_OneLinersTest extends PHPUnit_Framework_TestCase
{

	/**
	 * @var Keboola\StorageApi\Client
	 */
	protected $_client;

	protected $_inBucketId;

	public function setUp()
	{
		// prepare bucket for tests
		$this->_client = new Keboola\StorageApi\Client(STORAGE_API_TOKEN, STORAGE_API_URL);
		$this->_inBucketId = $this->_initBucket('api-tests', 'in');

		Keboola\StorageApi\OneLiner::setClient($this->_client);
	}

	protected function _initBucket($name, $stage)
	{
		$bucketId = $this->_client->getBucketId('c-' . $name, $stage);
		if (!$bucketId) {
			$bucketId = $this->_client->createBucket($name, $stage, 'Api tests');
		}
		$tables = $this->_client->listTables($bucketId);
		foreach ($tables as $table) {
			$this->_client->dropTable($table['id']);
		}

		return $bucketId;
	}

	public function testOneLinerCreate()
	{
		$oneLiner = $this->uploadOneLiner();
		$this->assertEquals($oneLiner->id, 1);
		$this->assertEquals($oneLiner->name, "whatever");
	}

	public function testOneLinerLoad()
	{
		$oneLiner = $this->uploadOneLiner();
		$oneLiner->save();

		$oneLiner2 = new Keboola\StorageApi\OneLiner($this->_inBucketId . ".oneLinerTest");

		$this->assertEquals($oneLiner2->id, 1);
		$this->assertEquals($oneLiner2->name, "whatever");
	}

	private function uploadOneLiner()
	{
		$oneLiner = new Keboola\StorageApi\OneLiner($this->_inBucketId . ".oneLinerTest");
		$oneLiner->id = 1;
		$oneLiner->name = "whatever";
		return $oneLiner;
	}
}