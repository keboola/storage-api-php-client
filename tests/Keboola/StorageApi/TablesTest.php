<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

class Keboola_StorageApi_Buckets_TablesTest extends StorageApiTestCase
{

	protected $_inBucketId;
	protected $_outBucketId;


	public function setUp()
	{
		parent::setUp();

		$this->_outBucketId = $this->_initEmptyBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');
	}

	/**
	 * @dataProvider tableCreateData
	 * @param $langugesFile
	 */
	public function testTableCreate($langugesFile)
	{
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/' . $langugesFile);
		$table = $this->_client->getTable($tableId);

		$this->assertEquals($tableId, $table['id']);
		$this->assertEquals('languages', $table['name']);
		$this->assertEquals('languages', $table['gdName']);
		$this->assertNotEmpty($table['created']);
		$this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
		$this->assertEquals(count($this->_readCsv(__DIR__ . '/_data/languages.csv')) - 1, $table['rowsCount']);
		$this->assertNotEmpty($table['dataSizeBytes']);

		$this->assertEquals(file_get_contents(__DIR__ . '/_data/languages.csv'),
			$this->_client->exportTable($tableId), 'initial data imported into table');
	}

	public function tableCreateData()
	{
		return array(
			array('languages.csv'),
			array('languages.csv.gz'),
		);
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

	/**
	 * @dataProvider tableImportData
	 * @param $importFileName
	 */
	public function testTableImportExport($importFileName, $expectationsFileName, $colNames, $exportEscapeOutput = false)
	{
		$importFile = __DIR__ . '/_data/' . $importFileName;
		$expectationsFile = __DIR__ . '/_data/' . $expectationsFileName;
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', $expectationsFile);

		$result = $this->_client->writeTable($tableId, $importFile);
		$table = $this->_client->getTable($tableId);

		$rowsCountInCsv = count($this->_readCsv($expectationsFile)) - 1;
		$this->assertEmpty($result['warnings']);
		$this->assertEquals($colNames, array_values($result['importedColumns']), 'columns');
		$this->assertEmpty($result['transaction']);
		$this->assertEquals($rowsCountInCsv, $table['rowsCount'], 'rows count in csv');
		$this->assertNotEmpty($table['dataSizeBytes']);
		$this->assertEquals($rowsCountInCsv, $result['totalRowsCount'], 'rows count in csv result');
		$this->assertNotEmpty($result['totalDataSizeBytes']);

		// compare data
		$this->assertEquals(file_get_contents($expectationsFile), $this->_client->exportTable($tableId, null, null, null, $exportEscapeOutput), 'imported data comparsion');

		// incremental
		$result = $this->_client->writeTable($tableId, $importFile, null, ",", '"', true);
		$this->assertEquals(2 * $rowsCountInCsv, $result['totalRowsCount']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);
	}

	public function tableImportData()
	{
		return array(
			array('languages.csv', 'languages.csv', array('id', 'name')),
			array('languages.utf8.bom.csv', 'languages.csv', array('id', 'name')),
			array('languages.zip', 'languages.csv', array('id', 'name')),
			array('languages.csv.gz', 'languages.csv', array('id', 'name')),
			array('escaping.csv', 'escaping.standard.out.csv', array('col1', 'col2')),
			array('escaping.nl-last-row.csv', 'escaping.standard.out.csv', array('col1', 'col2')),
			array('escaping.csv', 'escaping.backslash.out.csv', array('col1', 'col2'), true),
		);
	}

	public function testTableFileExport()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);

		$outputFile = __DIR__ . '/_tmp/languagesExport.csv';
		if (file_exists($outputFile)) {
			unlink($outputFile);
		}

		// Full download
		$this->_client->exportTable($tableId, $outputFile);
		$this->assertFileEquals($importFile, $outputFile);
		unlink($outputFile);

		// Download with limit
		$this->_client->exportTable($tableId, $outputFile, 1);

		$this->assertEquals(exec("wc -l < " . escapeshellarg($outputFile)), "2");
		unlink($outputFile);
	}

	/**
	 * @dataProvider tableImportInvalidData
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableInvalidImport($languagesFile)
	{
		$importFile = __DIR__ . '/_data/' . $languagesFile;
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', __DIR__ . '/_data/languages.csv');

		$this->_client->writeTable($tableId, $importFile);
	}

	public function tableImportInvalidData()
	{
		return array(
			array('languages.invalid.csv'),
			array('languages.invalid.gzip'),
			array('languages.invalid.zip'),
		);
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
		$this->assertNotEmpty($aliasTable['created']);
		$this->assertNotEquals('0000-00-00 00:00:00', $aliasTable['created']);
		$this->assertEquals($sourceTable['rowsCount'], $aliasTable['rowsCount']);
		$this->assertEquals($sourceTable['dataSizeBytes'], $aliasTable['dataSizeBytes']);

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

	public function testTableAliasUnlink()
	{
		$importFile = __DIR__ . '/_data/languages.csv';

		// create and import data into source table
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);
		$this->_client->writeTable($sourceTableId, __DIR__ . '/_data/languages.csv');

		// create alias table
		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertArrayHasKey('sourceTable', $aliasTable);
		$this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id'], 'new table linked to source table');

		// unlink
		$this->_client->unlinkTable($aliasTableId);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertArrayNotHasKey('sourceTable', $aliasTable);

		// real table cannot be unlinked
		try {
			$this->_client->unlinkTable($aliasTableId);
			$this->fail('Real table should not be unlinked');
		} catch (\Keboola\StorageApi\ClientException $e) {
		}

	}

	public function testParseCsv()
	{
		$csvData = '"column1","column2"' . "\n" . '"value1","value2"';

		$data1 = \Keboola\StorageApi\Client::parseCsv($csvData);
		$data2 = \Keboola\StorageApi\Client::parseCsv($csvData, false);

		$this->assertEquals($data1[0]["column1"], "value1", 'Parse CSV');
		$this->assertEquals($data1[0]["column2"], "value2", 'Parse CSV');
		$this->assertEquals($data2[0][0], "column1", 'Parse CSV');
		$this->assertEquals($data2[0][1], "column2", 'Parse CSV');
		$this->assertEquals($data2[1][0], "value1", 'Parse CSV');
		$this->assertEquals($data2[1][1], "value2", 'Parse CSV');
	}

	/**
	 * @param $path
	 * @return array
	 */
	protected function _readCsv($path, $delimiter = ",", $enclosure = '"', $escape = '"')
	{
		$fh = fopen($path, 'r');
		$lines = array();
		while (($data = fgetcsv($fh, 1000, $delimiter, $enclosure, $escape)) !== FALSE) {
		  $lines[] = $data;
		}
		fclose($fh);
		return $lines;
	}

}