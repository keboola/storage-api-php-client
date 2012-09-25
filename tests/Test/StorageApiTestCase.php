<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.8.12
 * Time: 8:52
 *
 */

class StorageApiTestCase extends \PHPUnit_Framework_TestCase
{
	/**
	 * @var Keboola\StorageApi\Client
	 */
	protected $_client;

	public function setUp()
	{
		$this->_client = new Keboola\StorageApi\Client(STORAGE_API_TOKEN, STORAGE_API_URL);
	}

	/**
	 * Init empty bucket test helper
	 * @param $name
	 * @param $stage
	 * @return bool|string
	 */
	protected function _initEmptyBucket($name, $stage)
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

}
