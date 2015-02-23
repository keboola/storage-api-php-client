<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\Csv\CsvFile,
	Keboola\StorageApi\Client;

class Keboola_StorageApi_Tables_AliasesTest extends StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	public function testTableAlias()
	{
		$importFile = __DIR__ . '/../_data/languages.csv';

		// create and import data into source table
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$sourceTable = $this->_client->getTable($sourceTableId);

		$expectedData = Client::parseCsv(file_get_contents($importFile));
		usort($expectedData, function($a, $b) {
			return $a['id'] > $b['id'];
		});
		$this->assertEquals($expectedData, Client::parseCsv($this->_client->exportTable($sourceTableId)), 'data are present in source table');

		$results = $this->_client->exportTableAsync($sourceTableId);
		$file = $this->_client->getFile($results['file']['id']);
		$this->assertEquals($expectedData, Client::parseCsv(file_get_contents($file['url'])));

		// create alias table
		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'languages-alias');

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

		$results = $this->_client->exportTableAsync($aliasTableId);
		$file = $this->_client->getFile($results['file']['id']);
		$this->assertEquals($expectedData, Client::parseCsv(file_get_contents($file['url'])));

		// second import into source table
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$sourceTable = $this->_client->getTable($sourceTableId);
		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals($sourceTable['lastImportDate'], $aliasTable['lastImportDate']);

		// columns auto-create
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../_data/languages.more-columns.csv'));
		$sourceTable = $this->_client->getTable($sourceTableId);
		$expectedColumns = array(
			'id',
			'name',
			'count'
		);
		$this->assertEquals($expectedColumns, $sourceTable['columns'], 'Columns autocreate in source table');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals($expectedColumns, $aliasTable['columns'], 'Columns autocreate in alias table');

		// test creating alias from alias
		$callFailed = false;
		try {
			$this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $aliasTableId, 'double-alias');
		} catch (\Keboola\StorageApi\ClientException $e) {
			if ($e->getCode() == 400) {
				$callFailed = true;
			}
		}
		$this->assertTrue($callFailed, 'Alias of already aliased table should fail');

		$this->assertArrayHasKey('isAlias', $sourceTable);
		$this->assertFalse($sourceTable['isAlias']);
		$this->assertArrayHasKey('isAlias', $aliasTable);
		$this->assertTrue($aliasTable['isAlias']);


		try {
			$this->_client->dropTable($sourceTableId);
			$this->fail('Delete table with associated aliases should not been deleted');
		} catch (\Keboola\StorageApi\ClientException $e) {}

		// first delete alias, than source table
		$this->_client->dropTable($aliasTableId);
		$this->_client->dropTable($sourceTableId);
	}

	public function testRedshiftAliasUnsupportedMethods()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);

		$sql = 'SELECT name FROM "' . $testBucketId . '".languages LIMIT 2';
		$aliasTableId = $this->_client->createRedshiftAliasTable($this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT), $sql, null, $sourceTableId);

		try {
			$this->_client->setAliasTableFilter($aliasTableId, array('values' => array('VAN')));
			$this->fail('Setting of alias filter for redshift backend should fail');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals(501, $e->getCode());
		}

		try {
			$this->_client->removeAliasTableFilter($aliasTableId, array('values' => array('VAN')));
			$this->fail('Removing of alias filter for redshift backend should fail');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals(501, $e->getCode());
		}

		try {
			$this->_client->enableAliasTableColumnsAutoSync($aliasTableId);
			$this->fail('Columns syncing of alias filter for redshift backend should fail');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals(501, $e->getCode());
		}

		try {
			$this->_client->disableAliasTableColumnsAutoSync($aliasTableId);
			$this->fail('Columns syncing of alias filter for redshift backend should fail');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals(501, $e->getCode());
		}
	}

	public function testRedshiftAliasColumnsShouldNotBeSyncedOnSourceTableColumnAdd()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);
		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);

		$aliasTableId = $this->_client->createRedshiftAliasTable(
			$aliasBucketId,
			"SELECT name FROM \"$testBucketId\".languages",
			null,
			$sourceTableId
		);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals(array('name'), $aliasTable['columns']);

		$this->_client->addTableColumn($sourceTableId, 'created');
		$sourceTable = $this->_client->getTable($sourceTableId);
		$this->assertEquals(array('id', 'name', 'created'), $sourceTable['columns']);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals(array('name'), $aliasTable['columns']);
	}

	public function testRedshiftAliasTimestampColumnShouldBeAllowed()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);
		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);
		$aliasTableId = $this->_client->createRedshiftAliasTable(
			$aliasBucketId,
			"SELECT id, _timestamp FROM \"$testBucketId\".languages",
			'languages-alias'
		);

		$aliasTable = $this->_client->getTable($aliasTableId);

		$this->assertEquals(array('id'), $aliasTable['columns']);
	}

	public function testRedshiftAliasCanBeCreatedWithoutTimestampColumn()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);
		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);
		$aliasTableId = $this->_client->createRedshiftAliasTable(
			$aliasBucketId,
			"SELECT id FROM \"$testBucketId\".languages",
			'languages-alias'
		);


		$data = $this->_client->exportTable($aliasTableId);
		$this->assertNotEmpty($data);

		// sync export is not allowed
		try {
			$this->_client->exportTable($aliasTableId, null, array(
				'changedSince' => '-1 hour'
			));
			$this->fail('Export should throw exception');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation', $e->getStringCode());
		}

		// async export is not allowed
		try {
			$this->_client->exportTableAsync($aliasTableId, array(
				'changedSince' => '-1 hour'
			));
			$this->fail('Export should throw exception');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation', $e->getStringCode());
		}
	}

	public function testRedshiftInvalidSqlAliases()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);
		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);

		$this->_testAliasWithWrongSql($aliasBucketId, "SELECT name AS _name FROM \"$testBucketId\".languages"); // invalid column name
		$this->_testAliasWithWrongSql($aliasBucketId, "SELECT upper(name), upper(name) FROM \"$testBucketId\".languages"); // duplicate upper column
		$this->_testAliasWithWrongSql($aliasBucketId, "SELECT name FROM $testBucketId.languages LIMIT 2");
		$this->_testAliasWithWrongSql($aliasBucketId, "SELECT nonexistent FROM \"$testBucketId\".languages");
		$this->_testAliasWithWrongSql($aliasBucketId, "DELETE FROM \"$testBucketId\".languages");
		$this->_testAliasWithWrongSql($aliasBucketId, "SELECTX FROM \"$testBucketId\".languages");
		$this->_testAliasWithWrongSql($aliasBucketId, "SELECT name FROM $testBucketId.languages LIMIT 2;DELETE FROM \"$testBucketId\".languages");
	}

	private function _testAliasWithWrongSql($aliasBucketId, $sql)
	{
		try {
			$this->_client->createRedshiftAliasTable($aliasBucketId, $sql, uniqid());
			$this->fail('Alias with such sql should fail: ' . $sql);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('buckets.cannotCreateAliasFromSql', $e->getStringCode());
		}
	}

	public function testRedshiftAliases()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);
		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);

		$sql = "SELECT name FROM \"$testBucketId\".languages WHERE name='czech'";
		$aliasTableId = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, null, $sourceTableId);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertArrayHasKey('selectSql', $aliasTable);
		$this->assertEquals($sql, $aliasTable['selectSql']);
		$this->assertArrayHasKey('isAlias', $aliasTable);
		$this->assertEquals(1, $aliasTable['isAlias']);
		$this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id']);

		$data = $this->_client->exportTable($aliasTableId);
		$parsedData = Client::parseCsv($data, false);
		$this->assertEquals(2, count($parsedData));
		$this->assertEquals(array('czech'), $parsedData[1]);

		$sql2 = "SELECT name FROM \"$testBucketId\".languages WHERE name='english'";
		$this->_client->updateRedshiftAliasTable($aliasTableId, $sql2);

		$data = $this->_client->exportTable($aliasTableId);
		$parsedData = Client::parseCsv($data, false);
		$this->assertEquals(2, count($parsedData));
		$this->assertEquals(array('english'), $parsedData[1]);

		$this->_client->dropTable($aliasTableId);


		// test join
		$importFile = __DIR__ . '/../_data/languages.csv';
		$this->_client->createTable(
			$testBucketId,
			'languages2',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);
		$sql = "SELECT l1.name AS name1, l2.name AS name2 FROM \"$testBucketId\".languages l1 LEFT JOIN \"$testBucketId\".languages l2 ON (l1.id=l2.id) WHERE l1.name LIKE 'f%'";
		$aliasTableId = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, 'test2');

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEquals($sql, $aliasTable['selectSql']);
		$this->assertArrayNotHasKey('sourceTable', $aliasTable);
		$data = $this->_client->exportTable($aliasTableId);
		$parsedData = Client::parseCsv($data, false);
		$this->assertGreaterThanOrEqual(1, $parsedData);
		$this->assertEquals(array('name1', 'name2'), current($parsedData));

		$this->_client->dropTable($aliasTableId);
	}

	public function testRedshiftAliasLastImportDateOfAliasIsNotChangedAfterImportToSourceTable()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);

		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);
		$aliasTableId = $this->_client->createRedshiftAliasTable(
			$aliasBucketId,
			"SELECT name FROM \"$testBucketId\".languages",
			'languages',
			$sourceTableId
		);

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEmpty($aliasTable['lastChangeDate']);
		$this->assertEmpty($aliasTable['lastImportDate']);

		// import data into source table
		$this->_client->writeTable($sourceTableId, new CsvFile($importFile));

		$aliasTable = $this->_client->getTable($aliasTableId);
		$this->assertEmpty($aliasTable['lastImportDate']);
	}

	public function testRedshiftAliasAsyncExport()
	{
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$this->_client->createTable(
			$testBucketId,
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);

		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);
		$aliasTableId = $this->_client->createRedshiftAliasTable(
			$aliasBucketId,
			"SELECT id, name FROM \"$testBucketId\".users",
			'users'
		);

		$result = $this->_client->exportTableAsync($aliasTableId);
		$file = $this->_client->getFile($result['file']['id']);
		$this->assertNotEmpty(file_get_contents($file['url']));
	}

	public function testTableAliasFilterModifications()
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->getTestBucketId(self::STAGE_OUT),
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
		$importFile = __DIR__ . '/../_data/languages.csv';

		// create and import data into source table
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'));

		// create alias table
		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId);
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

	public function testAliasColumnWithoutAutoSyncShouldBeDeletable()
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
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
		$importFile =  __DIR__ . '/../_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
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
		$importFile =  __DIR__ . '/../_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users');

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
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'id');

		$aliasTable = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'languages', array(
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
		$importFile =  __DIR__ . '/../_data/users.csv';
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		$aliasTable = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
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
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'id');

		$aliasTable = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'languages', array(
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



	public function testAliasColumns()
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'name');

		$aliasColumns = array(
			'id',
			'city',
		);
		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->getTestBucketId(self::STAGE_OUT),
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
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');

		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->getTestBucketId(self::STAGE_OUT),
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

		$results = $this->_client->exportTableAsync($aliasTableId);
		$file = $this->_client->getFile($results['file']['id']);
		$parsedData = Client::parseCsv(file_get_contents($file['url']), false);
		array_shift($parsedData);
		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
	}


	/**
	 * Test case when alias is filtered but column with filter is not present in alias
	 */
	public function testFilteredAliasWithColumnsListed()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'sex');

		$aliasTableId = $this->_client->createAliasTable(
			$this->getTestBucketId(self::STAGE_OUT),
			$sourceTableId,
			'males',
			array(
				'aliasColumns' => array('id', 'name', 'city'),
				'aliasFilter' => array(
					'column' => 'sex',
					'values' => array('male'),
				),
			)
		);

		$expectedResult = array(
			array(
				"1",
				"martin",
				"PRG",
			),
			array(
				"3",
				"ondra",
				"VAN",
			),
			array(
				"4",
				"miro",
				"BRA",
			),
			array(
				"5",
				"hidden",
				"",
			)
		);

		$data =$this->_client->exportTable($aliasTableId);
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header
		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

		$results = $this->_client->exportTableAsync($aliasTableId);
		$file = $this->_client->getFile($results['file']['id']);
		$parsedData = Client::parseCsv(file_get_contents($file['url']), false);
		array_shift($parsedData);
		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
	}


	public function testFilterOnFilteredAlias()
	{
		// source table
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'city');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'sex');

		// alias table
		$aliasTableId = $this->_client->createAliasTable(
			$this->getTestBucketId(self::STAGE_OUT),
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

		$results = $this->_client->exportTableAsync($aliasTableId, array(
			'whereColumn' => 'sex',
			'whereValues' => array('male'),
		));
		$file = $this->_client->getFile($results['file']['id']);
		$parsedData = Client::parseCsv(file_get_contents($file['url']), false);
		array_shift($parsedData); // remove header
		$this->assertEquals($expectedResult, $parsedData);

		$data = $this->_client->exportTable($aliasTableId, null, array(
			'whereColumn' => 'city',
			'whereValues'=> array('VAN'),
		));
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header

		$this->assertEmpty($parsedData, 'Export filter should not overload alias filter');

		$results = $this->_client->exportTableAsync($aliasTableId, array(
			'whereColumn' => 'city',
			'whereValues'=> array('VAN'),
		));
		$file = $this->_client->getFile($results['file']['id']);
		$parsedData = Client::parseCsv(file_get_contents($file['url']), false);
		array_shift($parsedData); // remove header
		$this->assertEmpty($parsedData);
	}

	public function testAliasingToSysStageShouldNotBeEnabled()
	{
		$sysBucketId = $this->_initEmptyBucket('tests', self::STAGE_SYS);
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);

		try {
			$this->_client->createAliasTable($sysBucketId, $sourceTableId);
			$this->fail('create alias in sys stage should not be allowed.');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.buckets.invalidAliasStages', $e->getStringCode());
		}
	}

	public function testAliasingFromSysStageShouldNotBeEnabled()
	{
		$sysBucketId = $this->_initEmptyBucket('tests', self::STAGE_SYS);
		$sourceTableId = $this->_client->createTable(
			$sysBucketId,
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);

		try {
			$this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $sourceTableId);
			$this->fail('create alias in sys stage should not be allowed.');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.buckets.invalidAliasStages', $e->getStringCode());
		}
	}

	public function testAliasingBetweenInAndOutShouldBeAllowed()
	{
		$inTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);

		$aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $inTableId);
		$this->assertNotEmpty($aliasId, 'in -> out');
		$this->_client->dropTable($aliasId);

		$aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $inTableId, 'users-alias');
		$this->assertNotEmpty($aliasId, 'in -> in');
		$this->_client->dropTable($aliasId);

		$outTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_OUT),
			'users',
			new CsvFile(__DIR__ . '/../_data/users.csv')
		);

		$aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $outTableId, 'users-alias-from-out');
		$this->assertNotEmpty($aliasId, 'out -> out');
		$this->_client->dropTable($aliasId);

		$aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $outTableId, 'users-alias-from-out');
		$this->assertNotEmpty($aliasId, 'out -> in');
		$this->_client->dropTable($aliasId);
	}

}