<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 7/22/13
 * Time: 1:50 PM
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_SnapshottingTest extends StorageApiTestCase
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
	public function testTableSnapshotCreate($backend)
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, $backend),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$this->_client->setTableAttribute($tableId, 'first', 'some value');
		$this->_client->setTableAttribute($tableId, 'second', 'other value');
		$table = $this->_client->getTable($tableId);

		$description = 'Test snapshot';
		$snapshotId = $this->_client->createTableSnapshot($tableId, $description);
		$this->assertNotEmpty($snapshotId);

		$snapshot = $this->_client->getSnapshot($snapshotId);

		$this->assertEquals($description, $snapshot['description']);
		$this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
		$this->assertEquals($table['columns'], $snapshot['table']['columns']);
		$this->assertEquals($table['indexedColumns'], $snapshot['table']['indexedColumns']);
		$this->assertEquals($table['attributes'], $snapshot['table']['attributes']);
		$this->assertArrayHasKey('creatorToken', $snapshot);
		$this->assertNotEmpty($snapshot['dataFileId']);
	}

	/**
	 * @dataProvider inOutBackends
	 * @param $backend
	 */
	public function testCreateTableFromSnapshot($inBackend, $outBackend)
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, $inBackend),
			'languages',
			new CsvFile(__DIR__ . '/../_data/escaping.csv'),
			array(
				'primaryKey' => 'col1',
			)
		);

		$this->_client->setTableAttribute($sourceTableId, 'first', 'some value');
		$this->_client->setTableAttribute($sourceTableId, 'second', 'other value');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'col2_with_space');
		$sourceTable = $this->_client->getTable($sourceTableId);

		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);
		$newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(self::STAGE_OUT, $outBackend), $snapshotId);
		$newTable = $this->_client->getTable($newTableId);

		$this->assertEquals($sourceTable['name'], $newTable['name']);
		$this->assertEquals($sourceTable['primaryKey'], $newTable['primaryKey']);
		$this->assertEquals($sourceTable['columns'], $newTable['columns']);
		$this->assertEquals($sourceTable['indexedColumns'], $newTable['indexedColumns']);
		$this->assertEquals($sourceTable['transactional'], $newTable['transactional']);
		$this->assertEquals($sourceTable['attributes'], $newTable['attributes']);

		$this->assertLinesEqualsSorted($this->_client->exportTable($sourceTableId), $this->_client->exportTable($newTableId));
	}

	public function inOutBackends()
	{
		return array(
			array('mysql', 'mysql'),
			array('mysql', 'redshift'),
			array('redshift', 'mysql'),
			array('redshift', 'redshift'),
		);
	}


	public function testCreateTableFromSnapshotWithDifferentName()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);
		$sourceTable = $this->_client->getTable($sourceTableId);
		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);

		$newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(), $snapshotId, 'new-table');
		$newTable = $this->_client->getTable($newTableId);

		$this->assertEquals('new-table', $newTable['name']);
	}

	public function testAliasSnapshotCreateShouldNotBeAllowed()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId);

		try {
			$this->_client->createTableSnapshot($aliasTableId);
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.aliasSnapshotNotAllowed', $e->getStringCode());
		}
	}

	public function testGetTableSnapshot()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$this->_client->createTableSnapshot($sourceTableId, 'my snapshot');
		$snapshotId = $this->_client->createTableSnapshot($sourceTableId, 'second');

		$snapshots = $this->_client->listTableSnapshots($sourceTableId, array(
			'limit' => 2,
		));
		$this->assertInternalType('array', $snapshots);
		$this->assertCount(2, $snapshots);

		$newestSnapshot = reset($snapshots);
		$this->assertEquals($snapshotId, $newestSnapshot['id']);
		$this->assertEquals('second', $newestSnapshot['description']);
	}


	/**
	 * @param $backend
	 */
	public function testTableRollbackFromSnapshot()
	{
		$backend = self::BACKEND_MYSQL;
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, $backend),
			'languages',
			new CsvFile(__DIR__ . '/../_data/escaping.csv')
		);

		$this->_client->markTableColumnAsIndexed($sourceTableId, 'col1');
		$this->_client->setTableAttribute($sourceTableId, 'first', 'value');
		$this->_client->setTableAttribute($sourceTableId, 'second', 'another');

		$tableInfo = $this->_client->getTable($sourceTableId);
		$tableData = $this->_client->exportTable($sourceTableId);

		// create snapshot
		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);

		// do some modifications
		$this->_client->deleteTableAttribute($sourceTableId, 'first');
		$this->_client->removeTableColumnFromIndexed($sourceTableId, 'col1');
		$this->_client->setTableAttribute($sourceTableId, 'third', 'my value');
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../_data/escaping.csv'), array(
			'incremental' => true,
		));
		$this->_client->addTableColumn($sourceTableId, 'new_column');

		$aliasTableId = null;
		if ($backend == self::BACKEND_MYSQL) {
			$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT, $backend), $sourceTableId);
		}

		// and rollback to snapshot
		$tableInfoBeforeRollback = $this->_client->getTable($sourceTableId);
		$this->_client->rollbackTableFromSnapshot($sourceTableId, $snapshotId);

		$tableInfoAfterRollback = $this->_client->getTable($sourceTableId);
		if ($aliasTableId) {
			$aliasInfo = $this->_client->getTable($aliasTableId);
		}

		$this->assertEquals($tableInfo['columns'], $tableInfoAfterRollback['columns']);
		$this->assertEquals($tableInfo['primaryKey'], $tableInfoAfterRollback['primaryKey']);
		$this->assertEquals($tableInfo['primaryKey'], $aliasInfo['primaryKey']);
		$this->assertEquals($tableInfo['indexedColumns'], $tableInfoAfterRollback['indexedColumns']);
		$this->assertEquals($tableInfo['indexedColumns'], $aliasInfo['indexedColumns']);
		$this->assertEquals($tableInfo['attributes'], $tableInfoAfterRollback['attributes']);


		$this->assertNotEquals($tableInfoBeforeRollback['lastChangeDate'], $tableInfoAfterRollback['lastChangeDate']);
		$this->assertNotEquals($tableInfoBeforeRollback['lastImportDate'], $tableInfoAfterRollback['lastImportDate']);

		$this->assertEquals($tableData, $this->_client->exportTable($sourceTableId));

		if ($aliasTableId) {
			$this->assertEmpty($aliasInfo['attributes']);
			$this->assertEquals($tableInfo['columns'], $aliasInfo['columns']);
			$this->assertEquals($tableData, $this->_client->exportTable($aliasTableId));
		}
	}

	public function testRollbackShouldBeDeniedWhenThereAreFilteredAliasesOnColumnsNotIndexedInSnapshot()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);

		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);

		// add index and create filtered alias
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'name');
		$this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, null, array(
			'aliasFilter' => array(
				'column' => 'name',
				'values' => array('czech'),
			),
		));

		try {
			$this->_client->rollbackTableFromSnapshot($sourceTableId, $snapshotId);
			$this->fail('Rollback should be denied');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.missingIndexesInSnapshot', $e->getStringCode());
		}
	}

	public function testRollbackShouldBeDeniedWhenThereAreFilteredAliasesOnColumnsNotPresentInSnapshot()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);

		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);

		// add index and create filtered alias
		$this->_client->addTableColumn($sourceTableId, 'hermafrodit');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'hermafrodit');
		$this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, null, array(
			'aliasFilter' => array(
				'column' => 'hermafrodit',
				'values' => array('ano'),
			),
		));

		try {
			$this->_client->rollbackTableFromSnapshot($sourceTableId, $snapshotId);
			$this->fail('Rollback should be denied');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.missingColumnsInSnapshot', $e->getStringCode());
		}
	}

	public function testRollbackShouldBeDeniedWhenThereAreColumnsInAliasNotPresentInSnapshot()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);

		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);

		// add index and create filtered alias
		$this->_client->addTableColumn($sourceTableId, 'hermafrodit');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'hermafrodit');
		$aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, null);
		$this->_client->disableAliasTableColumnsAutoSync($aliasId);

		try {
			$this->_client->rollbackTableFromSnapshot($sourceTableId, $snapshotId);
			$this->fail('Rollback should be denied');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.missingColumnsInSnapshot', $e->getStringCode());
		}
	}

}