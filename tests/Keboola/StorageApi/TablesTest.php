<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

class Keboola_StorageApi_Buckets_TablesTest extends PHPUnit_Framework_TestCase
{

	/**
	 * @var Keboola\StorageApi\Client
	 */
	protected $_client;

	protected $_bucketId;


	public function setUp()
	{
		// prepare bucket for tests
		$this->_client = new Keboola\StorageApi\Client(STORAGE_API_TOKEN, STORAGE_API_URL);

		$bucketId = $this->_client->getBucketId('c-main', 'in');
		if (!$bucketId) {
			throw new Exception('bucket in.c-main not found');
		}
		$tables = $this->_client->listTables($bucketId);
		foreach ($tables as $table) {
			$this->_client->dropTable($table['id']);
		}

		$this->_bucketId = $bucketId;
	}


	public function testTableCreate()
	{
		$tableId = $this->_client->createTable($this->_bucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$table = $this->_client->getTable($tableId);

		$this->assertEquals($tableId, $table['id']);
		$this->assertEquals('languages', $table['name']);
		$this->assertEquals('languages', $table['gdName']);

	}

	public function testTableImport()
	{
		$importFile = __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_bucketId, 'languages', $importFile);

		$result = $this->_client->writeTable($tableId, __DIR__ . '/_data/languages.csv');

		$this->assertEmpty($result['warnings']);
		$this->assertEquals(array('id', 'name'), $result['importedColumns']);
		$this->assertEmpty($result['transaction']);

		// compare data
		$dataInTable = array_map('str_getcsv', explode("\n", $this->_client->exportTable($tableId)));
		$expectedData  =  array_map('str_getcsv', explode("\n", file_get_contents($importFile)));

		$this->assertEquals($expectedData, $dataInTable);
	}

}