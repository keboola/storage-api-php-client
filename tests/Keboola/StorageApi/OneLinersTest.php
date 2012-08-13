<?php
/**
 *
 * User: Ondřej Hlaváček
 * Date: 7.8.12
 * Time: 16:40
 *
 */

class Keboola_StorageApi_OneLinersTest extends StorageApiTestCase
{

	protected $_inBucketId;

	public function setUp()
	{
		parent::setUp();
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');

		Keboola\StorageApi\OneLiner::setClient($this->_client);
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