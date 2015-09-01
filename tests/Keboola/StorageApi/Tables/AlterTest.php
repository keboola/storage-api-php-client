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