<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_AlterTest extends StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableColumnAdd($backend)
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new CsvFile($importFile));

		$this->_client->addTableColumn($tableId, 'State');

		$detail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('columns', $detail);
		$this->assertContains('State', $detail['columns']);
		$this->assertEquals(array('id','name','State'), $detail['columns']);
	}

	/**
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableExistingColumnAdd()
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));
		$this->_client->addTableColumn($tableId, 'id');
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableColumnDelete($backend)
	{
		$importFile =  __DIR__ . '/../_data/languages.camel-case-columns.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new CsvFile($importFile));

		$this->_client->deleteTableColumn($tableId, 'Name');

		$detail = $this->_client->getTable($tableId);
		$this->assertEquals(array('Id'), $detail['columns']);

		try {
			$this->_client->deleteTableColumn($tableId, 'Id');
			$this->fail("Exception should be thrown when last column is remaining");
		} catch (\Keboola\StorageApi\ClientException $e) {
		}
	}

	public function testTablePkColumnDelete()
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
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

	public function testPrimaryKeyAddRequiredParam()
	{
		$indexColumn = 'city';
		$primaryKeyColumns = array();
		$importFile = __DIR__ . '/../_data/users.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile($importFile),
			array()
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$tables = array(
			$this->_client->getTable($tableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEmpty($tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
		}


		try {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
		} catch (\Keboola\StorageApi\ClientException $e) {
			if ($e->getStringCode() !== 'storage.validation.primaryKey') {
				throw $e;
			}
		}

		$tables = array(
			$this->_client->getTable($tableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEmpty($tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
		}
	}

	/**
	 * Tests: https://github.com/keboola/connection/issues/202
	 */
	public function testPrimaryKeyAddTooLong()
	{
		$importFile = __DIR__ . '/../_data/multiple-columns-pk.csv';

		$primaryKeyColumns = array(
			"Paid_Search_Engine_Account","Advertiser_ID","Date","Paid_Search_Campaign","Paid_Search_Ad_ID","Site__DFA"
		);
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'longPK',
			new CsvFile($importFile),
			array()
		);

		try  {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
			$this->fail('create should fail as key will be too long');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyTooLong', $e->getStringCode());
		}
	}

	/**
	 * Tests: https://github.com/keboola/connection/issues/246
	 */
	public function testPrimaryKeyAddWithSameColumnsInDifferentBuckets()
	{
		$importFile = __DIR__ . '/../_data/users.csv';

		$table1Id = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'users',
			new CsvFile($importFile)
		);

		$this->_client->addTableColumn($table1Id, 'new-column');

		$table2Id = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT),
			'users',
			new CsvFile($importFile)
		);

		$this->_client->createTablePrimaryKey($table2Id, ['id']);

		$table = $this->_client->getTable($table2Id);

		$this->assertEquals(['id'], $table['primaryKey']);
	}

	public function testPrimaryKeyAddWithDuplicty()
	{
		$primaryKeyColumns = array('id');
		$importFile = __DIR__ . '/../_data/users.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'users',
			new CsvFile($importFile),
			array()
		);

		$this->_client->writeTableAsync(
			$tableId,
			new CsvFile($importFile),
			array(
				'incremental' => true,
			)
		);

		try  {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
		}

		// composite primary key
		$primaryKeyColumns = array('Id', 'Name');
		$importFile = __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile($importFile),
			array()
		);

		$this->_client->writeTableAsync(
			$tableId,
			new CsvFile($importFile),
			array(
				'incremental' => true,
			)
		);

		try  {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
			$this->fail('create should not be allowed');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
		}
	}

	public function testRedshiftPrimaryKeyAddWithDuplicty()
	{
		$primaryKeyColumns = array('id');
		$importFile = __DIR__ . '/../_data/users.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'users',
			new CsvFile($importFile),
			array()
		);

		$this->_client->writeTableAsync(
			$tableId,
			new CsvFile($importFile),
			array(
				'incremental' => true,
			)
		);

		try  {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
			$this->fail('create primary key should not be allowed');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
		}

		// composite primary key
		$primaryKeyColumns = array('Id', 'Name');
		$importFile = __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'languages',
			new CsvFile($importFile),
			array()
		);

		$this->_client->writeTableAsync(
			$tableId,
			new CsvFile($importFile),
			array(
				'incremental' => true,
			)
		);

		try  {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
			$this->fail('create should not be allowed');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
		}
	}


	public function testPrimaryKeyAdd()
	{
		$indexColumn = 'city';
		$primaryKeyColumns = array('id');
		$importFile = __DIR__ . '/../_data/users.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'Users',
			new CsvFile($importFile),
			array()
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEmpty($tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
		}

		$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array_merge(array($indexColumn), $primaryKeyColumns), $tableDetail['indexedColumns']);
		}

		// composite primary key
		$indexColumn = 'iso';
		$primaryKeyColumns = array('Id', 'Name');
		$importFile = __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'LanGuages',
			new CsvFile($importFile),
			array()
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEmpty($tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
		}

		$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array_merge(array($indexColumn), $primaryKeyColumns), $tableDetail['indexedColumns']);
		}

		// existing primary key
		$primaryKeyColumns = array('id');
		$importFile = __DIR__ . '/../_data/languages.more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'Languages-more-columns',
			new CsvFile($importFile),
			array(
				'primaryKey' => reset($primaryKeyColumns)
			)
		);

		$tableDetail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals($primaryKeyColumns, $tableDetail['indexedColumns']);

		try {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
			$this->fail('create should not be allowed');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyAlreadyExists', $e->getStringCode());
		}
	}
	
	public function testRedshiftPrimaryKeyAdd()
	{
		$primaryKeyColumns = array('id');
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/users.csv';


		$tableId = $this->_client->createTable(
			$testBucketId,
			'Users',
			new CsvFile($importFile),
			array()
		);

		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);
		$sql = "SELECT * FROM \"$testBucketId\".users WHERE id='1'";
		$aliasTableId = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, null, $tableId);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			if (!$tableDetail['isAlias']) {
				$this->assertArrayHasKey('primaryKey', $tableDetail);
				$this->assertEmpty($tableDetail['primaryKey']);

				$this->assertArrayHasKey('indexedColumns', $tableDetail);
				$this->assertEmpty($tableDetail['indexedColumns']);
			}
		}

		$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			if (!$tableDetail['isAlias']) {
				$this->assertArrayHasKey('primaryKey', $tableDetail);
				$this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

				$this->assertArrayHasKey('indexedColumns', $tableDetail);
				$this->assertEquals($primaryKeyColumns, $tableDetail['indexedColumns']);
			}
		}

		// composite primary key
		$primaryKeyColumns = array('Id', 'Name');
		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$testBucketId,
			'LanGuages',
			new CsvFile($importFile),
			array()
		);

		$aliasBucketId = $this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT);
		$sql = "SELECT * FROM \"$testBucketId\".languages WHERE id='1'";
		$aliasTableId = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, null, $tableId);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			if (!$tableDetail['isAlias']) {
				$this->assertArrayHasKey('primaryKey', $tableDetail);
				$this->assertEmpty($tableDetail['primaryKey']);

				$this->assertArrayHasKey('indexedColumns', $tableDetail);
				$this->assertEmpty($tableDetail['indexedColumns']);
			}
		}

		$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			if (!$tableDetail['isAlias']) {
				$this->assertArrayHasKey('primaryKey', $tableDetail);
				$this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

				$this->assertArrayHasKey('indexedColumns', $tableDetail);
				$this->assertEquals($primaryKeyColumns, $tableDetail['indexedColumns']);
			}
		}

		// existing primary key
		$primaryKeyColumn = 'id';
		$importFile = __DIR__ . '/../_data/languages.more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'Languages-More-Columns',
			new CsvFile($importFile),
			array(
				'primaryKey' => $primaryKeyColumn
			)
		);

		$tableDetail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals(array($primaryKeyColumn), $tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array($primaryKeyColumn), $tableDetail['indexedColumns']);

		try {
			$this->_client->createTablePrimaryKey($tableId, $primaryKeyColumn);
			$this->fail('show not be created');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.primaryKeyAlreadyExists', $e->getStringCode());
		}
	}

	public function testPrimaryKeyDelete()
	{
		$indexColumn = 'city';
		$importFile = __DIR__ . '/../_data/users.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'users',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEquals(array('id'), $tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array('id', $indexColumn), $tableDetail['indexedColumns']);
		}

		$this->_client->removeTablePrimaryKey($tableId);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEmpty($tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
		}


		// composite primary key
		$indexColumn = 'iso';
		$importFile =  __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => "Id,Name",
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$tableDetail =  $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals(array('Id', 'Name'), $tableDetail['primaryKey']);
		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array('Id', 'Name', $indexColumn), $tableDetail['indexedColumns']);

		$this->_client->removeTablePrimaryKey($tableId);

		$tableDetail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEmpty($tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);


		// delete primary key from table with filtered alias
		$indexColumn = 'name';
		$importFile = __DIR__ . '/../_data/languages.more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages-more-columns',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$aliasTableId = $this->_client->createAliasTable(
			$this->getTestBucketId(self::STAGE_OUT),
			$tableId,
			null,
			array(
				'aliasFilter' => array(
					'column' => 'id',
					'values' => array('1'),
				),
			)
		);

		$tables = array(
			$this->_client->getTable($tableId),
			$this->_client->getTable($aliasTableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEquals(array('id'), $tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array('id', $indexColumn), $tableDetail['indexedColumns']);
		}

		$indexRemoved = true;
		try {
			$this->_client->removeTablePrimaryKey($tableId);
		} catch (\Keboola\StorageApi\ClientException $e) {
			if ($e->getStringCode() == 'storage.tables.cannotRemoveReferencedColumnFromPrimaryKey') {
				$indexRemoved = false;
			} else {
				throw $e;
			}
		}

		// delete primary key from alias
		$this->assertFalse($indexRemoved);

		$tableDetail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals(array('id'), $tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array('id', $indexColumn), $tableDetail['indexedColumns']);

		$indexRemoved = true;
		try {
			$this->_client->removeTablePrimaryKey($aliasTableId);
		} catch (\Keboola\StorageApi\ClientException $e) {
			if ($e->getStringCode() == 'storage.tables.aliasImportNotAllowed') {
				$indexRemoved = false;
			} else {
				throw $e;
			}
		}

		$this->assertFalse($indexRemoved);

		$tableDetail = $this->_client->getTable($aliasTableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals(array('id'), $tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array('id', $indexColumn), $tableDetail['indexedColumns']);
	}

	public function testRedshiftPrimaryKeyDelete()
	{
		$indexColumn = 'iso';
		$importFile =  __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => "Id,Name",
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$tableDetail =  $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals(array('Id', 'Name'), $tableDetail['primaryKey']);
		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array('Id', 'Name', $indexColumn), $tableDetail['indexedColumns']);

		$this->_client->removeTablePrimaryKey($tableId);

		$tableDetail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEmpty($tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);


		return;


		$indexColumn = 'city';
		$importFile = __DIR__ . '/../_data/users.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'users',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);


		$tables = array(
			$this->_client->getTable($tableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEquals(array('id'), $tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array('id', $indexColumn), $tableDetail['indexedColumns']);
		}

		try {
			$this->_client->removeTablePrimaryKey($tableId);
		} catch (\Exception $e) {
			echo $e->getMessage();
			echo PHP_EOL;
			throw $e;
		}


		$tables = array(
			$this->_client->getTable($tableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEmpty($tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
		}


		// composite primary key
		$indexColumn = 'iso';
		$importFile =  __DIR__ . '/../_data/languages-more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => "Id,Name",
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$tableDetail =  $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEquals(array('Id', 'Name'), $tableDetail['primaryKey']);
		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array('Id', 'Name', $indexColumn), $tableDetail['indexedColumns']);

		$this->_client->removeTablePrimaryKey($tableId);

		$tableDetail = $this->_client->getTable($tableId);

		$this->assertArrayHasKey('primaryKey', $tableDetail);
		$this->assertEmpty($tableDetail['primaryKey']);

		$this->assertArrayHasKey('indexedColumns', $tableDetail);
		$this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);


		// delete primary key from table with filtered alias
		$indexColumn = 'name';
		$importFile = __DIR__ . '/../_data/languages.more-columns.csv';

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'languages-more-columns',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);

		$this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

		$tables = array(
			$this->_client->getTable($tableId),
		);

		foreach ($tables AS $tableDetail) {
			$this->assertArrayHasKey('primaryKey', $tableDetail);
			$this->assertEquals(array('id'), $tableDetail['primaryKey']);

			$this->assertArrayHasKey('indexedColumns', $tableDetail);
			$this->assertEquals(array('id', $indexColumn), $tableDetail['indexedColumns']);
		}

		$indexRemoved = true;
		try {
			$this->_client->removeTablePrimaryKey($tableId);
		} catch (\Keboola\StorageApi\ClientException $e) {
			if ($e->getStringCode() == 'storage.tables.cannotRemoveReferencedColumnFromPrimaryKey') {
				$indexRemoved = false;
			} else {
				throw $e;
			}
		}
	}

	public function testIndexedColumnsChanges()
	{
		$importFile = __DIR__ . '/../_data/users.csv';

		// create and import data into source table
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'users',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id'
			)
		);
		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

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
		$importFile = __DIR__ . '/../_data/more-columns.csv';

		// create and import data into source table
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));

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


}
