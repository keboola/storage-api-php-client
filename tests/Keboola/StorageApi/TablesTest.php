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

		$bucketId = $this->_client->getBucketId('c-api_tests', 'in');
		if (!$bucketId) {
			$bucketId = $this->_client->createBucket('api_tests', 'in', 'Api tests');
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

	public function testTableDelete()
	{
		$table1Id = $this->_client->createTable($this->_bucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$table2Id = $this->_client->createTable($this->_bucketId, 'languages_2', __DIR__ . '/_data/languages.csv');
		$tables = $this->_client->listTables();

		$this->assertCount(2, $tables);
		$this->_client->dropTable($table1Id);

		$tables = $this->_client->listTables();
		$this->assertCount(1, $tables);

		$table = reset($tables);
		$this->assertEquals($table2Id, $table['id']);
	}

	public function testTableImport()
	{
		$importFile = __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_bucketId, 'languages', $importFile);

		$result = $this->_client->writeTable($tableId, __DIR__ . '/_data/languages.csv');

		$this->assertEmpty($result['warnings']);
		$this->assertEquals(array('id', 'name'), array_values($result['importedColumns']), 'columns');
		$this->assertEmpty($result['transaction']);

		// compare data
		$dataInTable = array_map('str_getcsv', explode("\n", $this->_client->exportTable($tableId)));
		$expectedData  =  array_map('str_getcsv', explode("\n", file_get_contents($importFile)));

		$this->assertEquals($expectedData, $dataInTable, 'imported data comparsion');
	}

	/**
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableInvalidImport()
	{
		$importFile = __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_bucketId, 'languages', $importFile);

		$this->_client->writeTable($tableId, __DIR__ . '/_data/languages.invalid.csv');
	}

	public function testGoodDataXml()
	{
		$table1Id = $this->_client->createTable($this->_bucketId, 'languages', __DIR__ . '/_data/languages.csv');

		$doc = DOMDocument::loadXML($this->_client->getGdXmlConfig($table1Id));
		$this->assertEquals('schema', $doc->firstChild->tagName);
		$this->assertEquals(2, $doc->firstChild->childNodes->length);
	}

	public function testTableDefinition()
	{
		$table1Id = $this->_client->createTable($this->_bucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$sql = $this->_client->getTableDefinition($table1Id);
		$this->assertNotEmpty($sql);
		$this->_client->dropTable($table1Id);
	}

	public function testTableAttributes()
	{
		$tableId = $this->_client->createTable($this->_bucketId, 'languages', __DIR__ . '/_data/languages.csv');

		$table = $this->_client->getTable($tableId);
		$this->assertEmpty($table['attributes'], 'empty attributes after table create');

		// create
		$this->_client->setTableAttribute($tableId, 'something', 'lala');
		$this->_client->setTableAttribute($tableId, 'other', 'hello');
		$table = $this->_client->getTable($tableId);
		$this->assertEquals($table['attributes'], array('something' => 'lala', 'other' => 'hello'), 'attribute set');

		// update
		$this->_client->setTableAttribute($tableId, 'something', 'papa');
		$table = $this->_client->getTable($tableId);
		$this->assertEquals($table['attributes'], array('something' => 'papa', 'other' => 'hello'), 'attribute update');

		// delete
		$this->_client->deleteTableAttribute($tableId, 'something');
		$table = $this->_client->getTable($tableId);
		$this->assertEquals($table['attributes'], array('other' => 'hello'), 'attribute delete');
	}


}