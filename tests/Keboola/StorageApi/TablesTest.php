<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */


use Keboola\StorageApi\Client;

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_TablesTest extends StorageApiTestCase
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
		$tableId = $this->_client->createTable(
			$this->_inBucketId,
			'languages',
			new CsvFile($langugesFile)
		);
		$table = $this->_client->getTable($tableId);

		$this->assertEquals($tableId, $table['id']);
		$this->assertEquals('languages', $table['name']);
		$this->assertNotEmpty($table['created']);
		$this->assertNotEmpty($table['lastChangeDate']);
		$this->assertNotEmpty($table['lastImportDate']);
		$this->assertEquals(array("id", "name"), $table['columns']);
		$this->assertEmpty($table['indexedColumns']);
		$this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
		$this->assertEquals(count($this->_readCsv(__DIR__ . '/_data/languages.csv')) - 1, $table['rowsCount']);
		$this->assertNotEmpty($table['dataSizeBytes']);

		$this->assertEquals(file_get_contents(__DIR__ . '/_data/languages.csv'),
		$this->_client->exportTable($tableId), 'initial data imported into table');
	}

	public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated()
	{
		try {
			$tableId = $this->_client->createTable(
				$this->_inBucketId,
				'languages.main',
				new CsvFile(__DIR__ . '/_data/languages.csv')
			);
			$this->fail('Table with dot in name should not be created');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation', $e->getStringCode());
		}
	}

	public function testTableCreateWithPK()
	{
		$tableId = $this->_client->createTable(
			$this->_inBucketId,
			'languages',
			new CsvFile(__DIR__ . '/_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$table = $this->_client->getTable($tableId);
		$this->assertEquals(array('id'), $table['primaryKey']);
		$this->assertEquals(array('id'), $table['indexedColumns']);
	}

	public function tableCreateData()
	{
		return array(
			array(__DIR__ . '/_data/languages.csv'),
			array('https://s3.amazonaws.com/keboola-tests/languages.csv'),
			array(__DIR__ . '/_data/languages.csv.gz'),
		);
	}

	public function testListTables()
	{
		$this->_client->createTable($this->_inBucketId, 'languages', new CsvFile(__DIR__ . '/_data/languages.csv'));
		$tables = $this->_client->listTables($this->_inBucketId);

		$this->assertCount(1, $tables);

		$firstTable = reset($tables);
		$this->assertArrayHasKey('attributes', $firstTable, 'List bucket tables are returned with attributes');

		$tables = $this->_client->listTables();
		$firstTable = reset($tables);
		$this->assertArrayHasKey('attributes', $firstTable, 'List tables are returned with attributes');
	}

	public function testTableDelete()
	{
		$table1Id = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile(__DIR__ . '/_data/languages.csv'));
		$table2Id = $this->_client->createTable($this->_inBucketId, 'languages_2', new CsvFile(__DIR__ . '/_data/languages.csv'));
		$tables = $this->_client->listTables($this->_inBucketId);

		$this->assertCount(2, $tables);
		$this->_client->dropTable($table1Id);

		$tables = $this->_client->listTables($this->_inBucketId);
		$this->assertCount(1, $tables);

		$table = reset($tables);
		$this->assertEquals($table2Id, $table['id']);
	}

	/**
	 * @dataProvider tableImportData
	 * @param $importFileName
	 */
	public function testTableImportExport(CsvFile $importFile, $expectationsFileName, $colNames, $format = 'rfc')
	{
		$expectationsFile = __DIR__ . '/_data/' . $expectationsFileName;
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);

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
		$this->assertEquals(file_get_contents($expectationsFile), $this->_client->exportTable($tableId, null, array(
			'format' => $format,
		)), 'imported data comparsion');

		// incremental
		$result = $this->_client->writeTable($tableId,  $importFile, array(
			'incremental' => true,
		));
		$this->assertEquals(2 * $rowsCountInCsv, $result['totalRowsCount']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);
	}

	public function tableImportData()
	{
		return array(
			array(new CsvFile(__DIR__ . '/_data/languages.csv'), 'languages.csv', array('id', 'name')),
			array( new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv'), 'languages.csv', array('id', 'name')),
			array( new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('id', 'name')),
			array( new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.zip'), 'languages.csv', array('id', 'name')),
			array( new CsvFile(__DIR__ . '/_data/languages.utf8.bom.csv'), 'languages.csv', array('id', 'name')),
//			array( new CsvFile( __DIR__ . '/_data/languages.zip'), 'languages.csv', array('id', 'name')),
			array( new CsvFile(__DIR__ . '/_data/languages.csv.gz'), 'languages.csv', array('id', 'name')),
			array( new CsvFile(__DIR__ . '/_data/escaping.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),
			array( new CsvFile(__DIR__ . '/_data/escaping.nl-last-row.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),
			array( new CsvFile(__DIR__ . '/_data/escaping.csv'), 'escaping.backslash.out.csv', array('col1', 'col2_with_space'), 'escaped'),
			array( new CsvFile(__DIR__ . '/_data/escaping.csv'), 'escaping.raw.csv', array('col1', 'col2_with_space'), 'raw'),
			array( new CsvFile(__DIR__ . '/_data/escaping.raw.csv', "\t", "", "\\"), 'escaping.raw.csv', array('col1', 'col2_with_space'), 'raw'),
		);
	}

	public function testTableAsyncImport()
	{
		$importFile = new CsvFile(__DIR__ . '/_data/languages.csv');
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', $importFile);
		$result = $this->_client->writeTableAsync($tableId, $importFile, array(
			'incremental' => true,
		));

		$this->assertEquals('success', $result['status']);
	}

	public function testPartialImport()
	{
		$tableId = $this->_client->createTable(
			$this->_inBucketId, 'users',
			new CsvFile(__DIR__ . '/_data/users.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$this->_client->writeTable($tableId, new CsvFile(__DIR__ . '/_data/users-partial.csv'), array(
			'incremental' => true,
			'partial' => true,
		));

		$expectedData = Client::parseCsv(file_get_contents(__DIR__ . '/_data/users-partial-expected.csv'), false);
		$parsedData = Client::parseCsv($this->_client->exportTable($tableId), false);

		$this->assertEquals($expectedData, $parsedData);
	}

	public function testInvalidExportFormat()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));

		try {
			$this->_client->exportTable($tableId, null, array(
				'format' => 'csv',
			));
			$this->fail('Should throw exception');
		} catch(\Keboola\StorageApi\Exception $e) {
			$this->assertEquals('storage.tables.validation.invalidFormat', $e->getStringCode());
		}
	}

	public function testTableColumnAdd()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));

		$this->_client->addTableColumn($tableId, 'state');

		$detail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('columns', $detail);
		$this->assertContains('state', $detail['columns']);
		$this->assertEquals(array('id','name','state'), $detail['columns']);
	}

	/**
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableExistingColumnAdd()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));
		$this->_client->addTableColumn($tableId, 'id');
	}

	public function testTableColumnDelete()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));

		$this->_client->deleteTableColumn($tableId, 'name');

		$detail = $this->_client->getTable($tableId);
		$this->assertEquals(array('id'), $detail['columns']);

		try {
			$this->_client->deleteTableColumn($tableId, 'id');
			$this->fail("Exception should be thrown when last column is remaining");
		} catch (\Keboola\StorageApi\ClientException $e) {
		}
	}

	public function testAliasColumnWithoutAutoSyncShouldBeDeletable()
	{
		$importFile =  __DIR__ . '/_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'users', array(
			'aliasColumns' => array(
				'city',
				'id',
				'name',
			),
		));

		$this->_client->deleteTableColumn($aliasTableId, 'city');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals(array('id', 'name'), $aliasTable['columns']);
	}

	public function testAliasColumnWithoutAutoSyncCanBeAdded()
	{
		$importFile =  __DIR__ . '/_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'users', array(
			'aliasColumns' => array(
				'id',
				'name',
			),
		));

		$this->_client->addTableColumn($aliasTableId, 'city');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals(array('id', 'name', 'city'), $aliasTable['columns']);
	}

	public function testAliasColumnsAutoSync()
	{
		$importFile =  __DIR__ . '/_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'users');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals(array("id","name","city","sex"), $aliasTable["columns"]);

		$this->_client->addTableColumn($sourceTableId, 'age');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$expectedColumns = array("id","name","city","sex","age");
		$this->assertEquals($expectedColumns, $aliasTable["columns"]);

		$this->_client->disableAliasTableColumnsAutoSync($aliasTableId);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertFalse($aliasTable['aliasColumnsAutoSync']);

		$this->_client->addTableColumn($sourceTableId, 'birthDate');
		$this->_client->deleteTableColumn($aliasTableId, 'name');

		$aliasTable = $this->_client->getTable($aliasTableId);

		$expectedColumns = array("id","city","sex","age");
		$this->assertEquals($expectedColumns, $aliasTable["columns"]);

		$data = $this->_client->parseCsv($this->_client->exportTable($aliasTableId));
		$this->assertEquals($expectedColumns, array_keys(reset($data)));

		$this->_client->enableAliasTableColumnsAutoSync($aliasTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertEquals(array("id","name","city","sex","age","birthDate"), $aliasTable['columns']);
	}

	public function testColumnUsedInFilteredAliasShouldNotBeDeletable()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'id');

		$aliasTable = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'languages', array(
			'aliasFilter' => array(
				'column' => 'id',
				'values' => array('1'),
			),
		));

		try {
			$this->_client->deleteTableColumn($sourceTableId, 'id');
			$this->fail('Exception should be thrown when filtered column is deleted');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
		}
	}

	public function testColumnAssignedToAliasWithoutAutoSyncShouldNotBeDeletable()
	{
		$importFile =  __DIR__ . '/_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTable = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'users', array(
			'aliasColumns' => array(
				'city',
				'id',
				'name',
			),
		));

		try {
			$this->_client->deleteTableColumn($sourceTableId, 'city');
			$this->fail('Exception should be thrown when referenced column is deleted');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
		}
	}

	public function testColumnUsedInFilteredAliasShouldNotBeRemovedFromIndexed()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'id');

		$aliasTable = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'languages', array(
			'aliasFilter' => array(
				'column' => 'id',
				'values' => array('1'),
			),
		));

		try {
			$this->_client->removeTableColumnFromIndexed($sourceTableId, 'id');
			$this->fail('Exception should be thrown when filtered column is deleted');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.cannotRemoveReferencedColumnFromIndexed', $e->getStringCode());
		}
	}

	public function testTablePkColumnDelete()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable(
			$this->_inBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => "id,name",
			)
		);

		$detail =  $this->_client->getTable($tableId);

		$this->assertEquals(array('id', 'name'), $detail['primaryKey']);
		$this->assertEquals(array('id', 'name'), $detail['indexedColumns']);

		$this->_client->deleteTableColumn($tableId, 'name');
		$detail = $this->_client->getTable($tableId);

		$this->assertEquals(array('id'), $detail['columns']);

		$this->assertEquals(array('id'), $detail['primaryKey']);
		$this->assertEquals(array('id'), $detail['indexedColumns']);
	}

	public function testTableFileExport()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));

		$outputFile = __DIR__ . '/_tmp/languagesExport.csv';
		if (file_exists($outputFile)) {
			unlink($outputFile);
		}

		// Full download
		$this->_client->exportTable($tableId, $outputFile);
		$this->assertFileEquals($importFile, $outputFile);
		unlink($outputFile);

		// Download with limit
		$this->_client->exportTable($tableId, $outputFile, array(
			'limit' => 1,
		));

		$this->assertEquals(exec("wc -l < " . escapeshellarg($outputFile)), "2");
		unlink($outputFile);
	}

	public function testTableExportParams()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));

		$originalFileLinesCount = exec("wc -l <" . escapeshellarg($importFile));

		$data = $this->_client->exportTable($tableId);
		$this->assertEquals($originalFileLinesCount,  count(Client::parseCsv($data, false)));

		$data = $this->_client->exportTable($tableId, null, array(
			'limit' => 2,
		));
		$this->assertEquals(3, count(Client::parseCsv($data, false)), "limit parameter");

		sleep(10);
		$startTime = time();
		$importCsv = new \Keboola\Csv\CsvFile($importFile);
		$this->_client->writeTable($tableId, $importCsv, array(
			'incremental' => true,
		));
		$this->_client->writeTable($tableId, $importCsv, array(
			'incremental' => true,
		));
		$data = $this->_client->exportTable($tableId);
		$this->assertEquals((3 * ($originalFileLinesCount - 1)) + 1, count(Client::parseCsv($data ,false)), "lines count after incremental load");

		$data = $this->_client->exportTable($tableId, null, array(
			'changedSince' => sprintf('-%d second', ceil(time() - $startTime) + 5),
		));
		$this->assertEquals((2 * ($originalFileLinesCount - 1)) + 1, count(Client::parseCsv($data ,false)), "changedSince parameter");

		$data = $this->_client->exportTable($tableId, null, array(
			'changedUntil' => sprintf('-%d second', ceil(time() - $startTime) + 5),
		));
		$this->assertEquals($originalFileLinesCount, count(Client::parseCsv($data, false)), "changedUntil parameter");
	}

	/**
	 * @param $exportOptions
	 * @param $expectedResult
	 * @dataProvider tableExportFiltersData
	 */
	public function testTableExportFilters($exportOptions, $expectedResult)
	{
		$importFile =  __DIR__ . '/_data/users.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$data = $this->_client->exportTable($tableId, null, $exportOptions);
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header

		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
	}

	public function testAliasColumns()
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->_inBucketId,
			'users',
			new CsvFile(__DIR__ . '/_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'name');

		$aliasColumns = array(
			'id',
			'city',
		);
		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->_outBucketId,
			$sourceTableId,
			'users',
			array(
				'aliasColumns' => $aliasColumns,
			)
		);

		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertFalse($aliasTable['aliasColumnsAutoSync']);
		$this->assertEquals($aliasColumns, $aliasTable['columns']);
		$this->assertEquals(array('city'), $aliasTable['indexedColumns']);

		$this->_client->removeTableColumnFromIndexed($sourceTableId, 'city');
		$this->_client->addTableColumn($sourceTableId, 'another');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEmpty($aliasTable['indexedColumns'], 'Index should be removed also from alias');
		$this->assertEquals($aliasTable['columns'], $aliasColumns, 'Column should not be added to alias with auto sync disabled');
	}


	/**
	 * @param $filterOptions
	 * @param $expectedResult
	 * @dataProvider tableExportFiltersData
	 */
	public function testFilteredAliases($filterOptions, $expectedResult)
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->_inBucketId,
			'users',
			new CsvFile(__DIR__ . '/_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->_outBucketId,
			$sourceTableId,
			'users',
			array(
				'aliasFilter' => array(
					'column' => $filterOptions['whereColumn'],
					'operator' => isset($filterOptions['whereOperator']) ? $filterOptions['whereOperator'] : '',
					'values' => $filterOptions['whereValues'],
				),
			)
		);

		$data = $this->_client->exportTable($aliasTableId);
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header

		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
	}

	public function testFilterOnFilteredAlias()
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->_inBucketId,
			'users',
			new CsvFile(__DIR__ . '/_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'sex');

		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->_outBucketId,
			$sourceTableId,
			'users',
			array(
				'aliasFilter' => array(
					'column' => 'city',
					'values' => array('PRG'),
				),
			)
		);

		$expectedResult = array(
			array(
				"1",
				"martin",
				"PRG",
				"male"
			)
		);

		$data = $this->_client->exportTable($aliasTableId, null, array(
			'whereColumn' => 'sex',
			'whereValues' => array('male'),
		));
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header

		$this->assertEquals($expectedResult, $parsedData);

		$data = $this->_client->exportTable($aliasTableId, null, array(
			'whereColumn' => 'city',
			'whereValues'=> array('VAN'),
		));
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header

		$this->assertEmpty($parsedData, 'Export filter should not overload alias filter');
	}

	public function tableExportFiltersData()
	{
		return array(
			// first test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array('PRG')
				),
				array(
					array(
						"1",
						"martin",
						"PRG",
						"male"
					),
					array(
						"2",
						"klara",
						"PRG",
						"female",
					),
				),
			),
			// first test with defined operator
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array('PRG'),
					'whereOperator' => 'eq',
				),
				array(
					array(
						"1",
						"martin",
						"PRG",
						"male"
					),
					array(
						"2",
						"klara",
						"PRG",
						"female",
					),
				),
			),
			// second test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array('PRG', 'VAN')
				),
				array(
					array(
						"1",
						"martin",
						"PRG",
						"male"
					),
					array(
						"2",
						"klara",
						"PRG",
						"female",
					),
					array(
						"3",
						"ondra",
						"VAN",
						"male",
					),
				),
			),
			// third test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array('PRG'),
					'whereOperator' => 'ne'
				),
				array(
					array(
						"5",
						"hidden",
						"",
						"male",
					),
					array(
						"4",
						"miro",
						"BRA",
						"male",
					),
					array(
						"3",
						"ondra",
						"VAN",
						"male",
					),
				),
			),
			// fourth test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array('PRG', 'VAN'),
					'whereOperator' => 'ne'
				),
				array(
					array(
						"4",
						"miro",
						"BRA",
						"male",
					),
					array(
						"5",
						"hidden",
						"",
						"male",
					),
				),
			),
			// fifth test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array(''),
					'whereOperator' => 'eq'
				),
				array(
					array(
						"5",
						"hidden",
						"",
						"male",
					),
				),
			),
			// sixth test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array(''),
					'whereOperator' => 'ne'
				),
				array(
					array(
						"4",
						"miro",
						"BRA",
						"male",
					),
					array(
						"1",
						"martin",
						"PRG",
						"male"
					),
					array(
						"2",
						"klara",
						"PRG",
						"female",
					),
					array(
						"3",
						"ondra",
						"VAN",
						"male",
					),
				),
			),
		);
	}

	public function testTableExportFilterShouldFailOnNonIndexedColumn()
	{
		$importFile =  __DIR__ . '/_data/users.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));

		try {
			$this->_client->exportTable($tableId, null, array(
				'whereColumn' => 'city',
				'whereValues' => array('PRG'),
			));
		} catch(\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.columnNotIndexed', $e->getStringCode());
		}
	}

	public function testTableExportColumnsParam()
	{
		$importFile =  __DIR__ . '/_data/languages.csv';
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));

		$data = $this->_client->exportTable($tableId, null, array(
			'columns' => array('id'),
		));
		$parsed = Client::parseCsv($data, false);
		$firstRow = reset($parsed);

		$this->assertCount(1, $firstRow);
		$this->assertArrayHasKey(0, $firstRow);
		$this->assertEquals("id", $firstRow[0]);
	}

	/**
	 * @dataProvider tableImportInvalidData
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableInvalidImport($languagesFile)
	{
		$importCsvFile = new CsvFile(__DIR__ . '/_data/' . $languagesFile);
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile(__DIR__ . '/_data/languages.csv'));

		$this->_client->writeTable($tableId, $importCsvFile);
	}

	public function tableImportInvalidData()
	{
		return array(
			array('languages.invalid.csv'),
			array('languages.invalid.gzip'),
			array('languages.invalid.zip'),
			array('languages.invalid.duplicateColumns.csv'),
		);
	}

	/**
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableNotExistsImport()
	{
		$importCsvFile = new CsvFile(__DIR__ . '/_data/languages.csv');
		$this->_client->writeTable('languages', $importCsvFile);
	}

	public function testTableAttributes()
	{
		$tableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile(__DIR__ . '/_data/languages.csv'));

		$table = $this->_client->getTable($tableId);
		$this->assertEmpty($table['attributes'], 'empty attributes after table create');

		// create
		$this->_client->setTableAttribute($tableId, 'something', 'lala');
		$this->_client->setTableAttribute($tableId, 'other', 'hello', true);
		$table = $this->_client->getTable($tableId);


		$this->assertArrayEqualsSorted($table['attributes'], array(
				array(
					'name' => 'something',
					'value' => 'lala',
					'protected' => false,
				),
				array(
					'name' => 'other',
					'value' => 'hello',
					'protected' => true,
				),
			), 'name', 'attribute set');

		// update
		$this->_client->setTableAttribute($tableId, 'something', 'papa');
		$table = $this->_client->getTable($tableId);
		$this->assertArrayEqualsSorted($table['attributes'], array(
			array(
				'name' => 'something',
				'value' => 'papa',
				'protected' => false,
			),
			array(
				'name' => 'other',
				'value' => 'hello',
				'protected' => true,
			),
		), 'name', 'attribute update');

		// delete
		$this->_client->deleteTableAttribute($tableId, 'something');
		$table = $this->_client->getTable($tableId);
		$this->assertArrayEqualsSorted($table['attributes'], array(
			array(
				'name' => 'other',
				'value' => 'hello',
				'protected' => true,
			),
		), 'attribute delete');

		$this->_client->deleteTableAttribute($tableId, 'other');
	}

	public function testTableAlias()
	{
		$importFile = __DIR__ . '/_data/languages.csv';

		// create and import data into source table
		$sourceTableId = $this->_client->createTable(
			$this->_inBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/_data/languages.csv'));
		$sourceTable = $this->_client->getTable($sourceTableId);

		$expectedData = Client::parseCsv(file_get_contents($importFile));
		usort($expectedData, function($a, $b) {
			return $a['id'] > $b['id'];
		});
		$this->assertEquals($expectedData, Client::parseCsv($this->_client->exportTable($sourceTableId)), 'data are present in source table');

		// create alias table
		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId, 'languages-alias');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertNotEmpty($sourceTable['lastImportDate']);
		$this->assertEquals($sourceTable['lastImportDate'], $aliasTable['lastImportDate']);
		$this->assertEquals($sourceTable['lastChangeDate'], $aliasTable['lastChangeDate']);
		$this->assertEquals($sourceTable['columns'], $aliasTable['columns']);
		$this->assertEquals($sourceTable['indexedColumns'], $aliasTable['indexedColumns']);
		$this->assertEquals($sourceTable['primaryKey'], $aliasTable['primaryKey']);
		$this->assertNotEmpty($aliasTable['created']);
		$this->assertNotEquals('0000-00-00 00:00:00', $aliasTable['created']);
		$this->assertEquals($sourceTable['rowsCount'], $aliasTable['rowsCount']);
		$this->assertEquals($sourceTable['dataSizeBytes'], $aliasTable['dataSizeBytes']);
		$this->assertTrue($aliasTable['aliasColumnsAutoSync']);

		$this->assertArrayHasKey('sourceTable', $aliasTable);
		$this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id'], 'new table linked to source table');
		$this->assertEquals($expectedData, Client::parseCsv($this->_client->exportTable($aliasTableId)), 'data are exported from source table');

		// second import into source table
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/_data/languages.csv'));
		$sourceTable = $this->_client->getTable($sourceTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals($sourceTable['lastImportDate'], $aliasTable['lastImportDate']);

		// columns auto-create
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/_data/languages.more-columns.csv'));
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

	public function testTableAliasFilterModifications()
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->_inBucketId,
			'users',
			new CsvFile(__DIR__ . '/_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->_outBucketId,
			$sourceTableId,
			'users',
			array(
				'aliasFilter' => array(
					'column' => 'city',
					'values' => array('PRG'),
					'operator' => 'eq',
				),
			)
		);

		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertEquals('city', $aliasTable['aliasFilter']['column']);
		$this->assertEquals(array('PRG'), $aliasTable['aliasFilter']['values']);
		$this->assertEquals('eq', $aliasTable['aliasFilter']['operator']);

		$this->assertNull($aliasTable['dataSizeBytes'], 'Filtered alias should have unknown size');
		$this->assertNull($aliasTable['rowsCount'], 'Filtered alias should have unknown rows count');

		$aliasTable = $this->_client->setAliasTableFilter($aliasTableId, array(
			'values' => array('VAN'),
		));

		$this->assertEquals('city', $aliasTable['aliasFilter']['column']);
		$this->assertEquals(array('VAN'), $aliasTable['aliasFilter']['values']);
		$this->assertEquals('eq', $aliasTable['aliasFilter']['operator']);


		try {
			$this->_client->setAliasTableFilter($aliasTableId, array(
				'column' => 'name',
			));
			$this->fail('Filter cannot be applied on column without index');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.columnNotIndexed', $e->getStringCode());
		}

		$this->_client->removeAliasTableFilter($aliasTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertArrayNotHasKey('aliasFilter', $aliasTable);

	}

	public function testTableAliasUnlink()
	{
		$importFile = __DIR__ . '/_data/languages.csv';

		// create and import data into source table
		$sourceTableId = $this->_client->createTable($this->_inBucketId, 'languages', new CsvFile($importFile));
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/_data/languages.csv'));

		// create alias table
		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $sourceTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertArrayHasKey('sourceTable', $aliasTable);
		$this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id'], 'new table linked to source table');

		// unlink
		$this->_client->unlinkTable($aliasTableId);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertArrayNotHasKey('sourceTable', $aliasTable);
		$this->assertEmpty($aliasTable['lastImportDate'], 'Last import date is null');
		$this->assertEquals(0, $aliasTable['dataSizeBytes']);
		$this->assertEquals(0, $aliasTable['rowsCount']);

		// real table cannot be unlinked
		try {
			$this->_client->unlinkTable($aliasTableId);
			$this->fail('Real table should not be unlinked');
		} catch (\Keboola\StorageApi\ClientException $e) {
		}

	}

	public function testParseCsv()
	{
		$csvData = '"column1","column2"' . PHP_EOL
			. '"valu\ "",e1","value2"' . PHP_EOL
			. '"new'  . PHP_EOL . 'line","col2"'
		;

		$expectedSimple = array(
			array(
				"column1",
				"column2",
			),
			array(
				'valu\ ",e1', 'value2',
			),
			array(
				"new\nline","col2",
			),
		);
		$expectedHashmap = array(
			array(
				"column1" => 'valu\ ",e1',
				"column2" => 'value2',
			),
			array(
				"column1" => "new\nline",
				"column2" => "col2",
			),
		);


		$data = \Keboola\StorageApi\Client::parseCsv($csvData, false);
		$this->assertEquals($expectedSimple, $data, "Csv parse to flat array");

		$data = \Keboola\StorageApi\Client::parseCsv($csvData, true);
		$this->assertEquals($expectedHashmap, $data, "Csv parse to associative array");
	}

	public function testIndexedColumnsChanges()
	{
		$importFile = __DIR__ . '/_data/users.csv';

		// create and import data into source table
		$tableId = $this->_client->createTable(
			$this->_inBucketId,
			'users',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);
		$aliasTableId = $this->_client->createAliasTable($this->_outBucketId, $tableId);

		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$detail = $this->_client->getTable($tableId);
		$aliasDetail = $this->_client->getTable($aliasTableId);

		$this->assertEquals(array("id", "city"), $detail['indexedColumns'], "Primary key is indexed with city column");
		$this->assertEquals(array("id", "city"), $aliasDetail['indexedColumns'], "Primary key is indexed with city column in alias Table");

		$this->_client->removeTableColumnFromIndexed($tableId, 'city');
		$detail = $this->_client->getTable($tableId);
		$aliasDetail = $this->_client->getTable($aliasTableId);

		$this->assertEquals(array("id"), $detail['indexedColumns']);
		$this->assertEquals(array("id"), $aliasDetail['indexedColumns']);

		try {
			$this->_client->removeTableColumnFromIndexed($tableId, 'id');
			$this->fail('Primary key should not be able to remove from indexed columns');
		} catch (\Keboola\StorageApi\ClientException $e) {

		}

		$this->_client->dropTable($aliasTableId);
		$this->_client->dropTable($tableId);
	}

	public function testIndexedColumnsCountShouldBeLimited()
	{
		$importFile = __DIR__ . '/_data/more-columns.csv';

		// create and import data into source table
		$tableId = $this->_client->createTable($this->_inBucketId, 'users', new CsvFile($importFile));

		$this->_client->markTableColumnAsIndexed($tableId, 'col1');
		$this->_client->markTableColumnAsIndexed($tableId, 'col2');
		$this->_client->markTableColumnAsIndexed($tableId, 'col3');
		$this->_client->markTableColumnAsIndexed($tableId, 'col4');

		try {
			$this->_client->markTableColumnAsIndexed($tableId, 'col5');
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.indexedColumnsCountExceed', $e->getStringCode());
		}
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