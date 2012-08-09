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

	protected $_inBucketId;
	protected $_outBucketId;


	public function setUp()
	{
		// prepare bucket for tests
		$this->_client = new Keboola\StorageApi\Client(STORAGE_API_TOKEN, STORAGE_API_URL);

		$this->_outBucketId = $this->_initBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initBucket('api-tests', 'in');
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


	public function testTableCreate()
	{
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$table = $this->_client->getTable($tableId);

		$this->assertEquals($tableId, $table['id']);
		$this->assertEquals('languages', $table['name']);
		$this->assertEquals('languages', $table['gdName']);

	}

	public function testTableDelete()
	{
		$table1Id = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$table2Id = $this->_client->createTable($this->_inBucketId, 'languages_2', __DIR__ . '/_data/languages.csv');
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
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);

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
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);

		$this->_client->writeTable($tableId, __DIR__ . '/_data/languages.invalid.csv');
	}

	public function testGoodDataXml()
	{
		$table1Id = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');

		$doc = DOMDocument::loadXML($this->_client->getGdXmlConfig($table1Id));
		$this->assertEquals('schema', $doc->firstChild->tagName);
		$this->assertEquals(2, $doc->firstChild->childNodes->length);
	}

	public function testTableDefinition()
	{
		$table1Id = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');
		$sql = $this->_client->getTableDefinition($table1Id);
		$this->assertNotEmpty($sql);
		$this->_client->dropTable($table1Id);
	}

	public function testTableAttributes()
	{
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');

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

	public function testTableAlias()
	{
		$importFile = __DIR__ . '/_data/languages.csv';

		// create and import data into source table
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);
		$this->_client->writeTable($sourceTableId, __DIR__ . '/_data/languages.csv');
		$sourceTable = $this->_client->getTable($sourceTableId);
		$this->assertEquals(file_get_contents($importFile), $this->_client->exportTable($sourceTableId), 'data are present in source table');

		// create alias table
		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertNotEmpty($sourceTable['lastImportDate']);
		$this->assertEquals($sourceTable['lastImportDate'], $aliasTable['lastImportDate']);
		$this->assertEquals($sourceTable['columns'], $aliasTable['columns']);

		$this->assertArrayHasKey('sourceTable', $aliasTable);
		$this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id'], 'new table linked to source table');
		$this->assertEquals(file_get_contents($importFile), $this->_client->exportTable($aliasTableId), 'data are exported from source table');

		// second import into source table
		$this->_client->writeTable($sourceTableId, __DIR__ . '/_data/languages.csv');
		$sourceTable = $this->_client->getTable($sourceTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals($sourceTable['lastImportDate'], $aliasTable['lastImportDate']);

		// columns auto-create
		$this->_client->writeTable($sourceTableId, __DIR__ . '/_data/languages.more-columns.csv');
		$sourceTable = $this->_client->getTable($sourceTableId);
		$expectedColumns = array(
			'id',
			'name',
			'count'
		);
		$this->assertEquals($expectedColumns, $sourceTable['columns'], 'Columns autocreate in source table');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals($expectedColumns, $aliasTable['columns'], 'Columns autocreate in alias table');

		try {
			$this->_client->dropTable($sourceTableId);
			$this->fail('Delete table with associated aliases should not been deleted');
		} catch (\Keboola\StorageApi\ClientException $e) {}

		// first delete alias, than source table
		$this->_client->dropTable($aliasTableId);
		$this->_client->dropTable($sourceTableId);
	}

	public function testParseCsv()
	{
		$csvData = '"column1","column2"' . "\n" . '"value1","value2"';
		$data1 = \Keboola\StorageApi\Client::parseCSV($csvData);
		$data2 = \Keboola\StorageApi\Client::parseCSV($csvData, false);
		$this->assertEquals($data1[0]["column1"], "value1", 'Parse CSV');
		$this->assertEquals($data1[0]["column2"], "value2", 'Parse CSV');
		$this->assertEquals($data2[0][0], "column1", 'Parse CSV');
		$this->assertEquals($data2[0][1], "column2", 'Parse CSV');
		$this->assertEquals($data2[1][0], "value1", 'Parse CSV');
		$this->assertEquals($data2[1][1], "value2", 'Parse CSV');
	}

}