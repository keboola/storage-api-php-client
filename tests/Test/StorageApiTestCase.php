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
		$this->_client = new Keboola\StorageApi\Client(array(
			'token' => STORAGE_API_TOKEN,
			'url' => STORAGE_API_URL,
			'backoffMaxTries' => 11,
		));
	}

	/**
	 * Init empty bucket test helper
	 * @param $name
	 * @param $stage
	 * @return bool|string
	 */
	protected function _initEmptyBucket($name, $stage, $backend = 'mysql')
	{
		try {
			$bucket = $this->_client->getBucket("$stage.c-$name");
			$tables = $this->_client->listTables($bucket['id']);
			foreach ($tables as $table) {
				$this->_client->dropTable($table['id']);
			}

			if ($bucket['backend'] != $backend) {
				$this->_client->dropBucket($bucket['id']);
				return $this->_client->createBucket($name, $stage, 'Api tests', $backend);
			}
			return $bucket['id'];
		} catch (\Keboola\StorageApi\ClientException $e) {
			return $this->_client->createBucket($name, $stage, 'Api tests', $backend);
		}
	}

	public  function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = "")
	{
		$comparsion = function($attrLeft, $attrRight) use($sortKey) {
			if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
				return 0;
			}
			return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
		};
		usort($expected, $comparsion);
		usort($actual, $comparsion);
		return $this->assertEquals($expected, $actual, $message);
	}

}
