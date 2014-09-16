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


	public function testTableSnapshotCreate()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
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

	public function testRedshiftTableSnapshotCreateShouldNotBeImplemented()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
				'columns' => array('id', 'name'),
			)
		);

		try {
			$this->_client->createTableSnapshot($tableId);
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals(501, $e->getCode());
			$this->assertEquals('notImplemented', $e->getStringCode());
		}
	}

	public function testCreateTableFromSnapshot()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$this->_client->setTableAttribute($sourceTableId, 'first', 'some value');
		$this->_client->setTableAttribute($sourceTableId, 'second', 'other value');
		$this->_client->markTableColumnAsIndexed($sourceTableId, 'name');
		$sourceTable = $this->_client->getTable($sourceTableId);

		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);
		$newTableId = $this->_client->createTableFromSnapshot($this->getTestBucketId(self::STAGE_OUT), $snapshotId);
		$newTable = $this->_client->getTable($newTableId);

		$this->assertEquals($sourceTable['name'], $newTable['name']);
		$this->assertEquals($sourceTable['primaryKey'], $newTable['primaryKey']);
		$this->assertEquals($sourceTable['columns'], $newTable['columns']);
		$this->assertEquals($sourceTable['indexedColumns'], $newTable['indexedColumns']);
		$this->assertEquals($sourceTable['transactional'], $newTable['transactional']);
		$this->assertEquals($sourceTable['attributes'], $newTable['attributes']);

		$this->assertEquals($this->_client->exportTable($sourceTableId), $this->_client->exportTable($newTableId));
	}

	public function testRedshiftTableCreateFromSnapshotShouldNotBeImplemented()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);
		try {
			$this->_client->createTableFromSnapshot($this->getTestBucketId(self::STAGE_OUT, self::BACKEND_REDSHIFT), $snapshotId);
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('notImplemented', $e->getStringCode());
		}
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


	public function testTableRollbackFromSnapshot()
	{
		$sourceTableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);

		$this->_client->markTableColumnAsIndexed($sourceTableId, 'name');
		$this->_client->setTableAttribute($sourceTableId, 'first', 'value');
		$this->_client->setTableAttribute($sourceTableId, 'second', 'another');

		$tableInfo = $this->_client->getTable($sourceTableId);
		$tableData = $this->_client->exportTable($sourceTableId);

		// create snapshot
		$snapshotId = $this->_client->createTableSnapshot($sourceTableId);

		// do some modifications
		$this->_client->deleteTableAttribute($sourceTableId, 'first');
		$this->_client->removeTableColumnFromIndexed($sourceTableId, 'name');
		$this->_client->setTableAttribute($sourceTableId, 'third', 'my value');
		$this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../_data/languages.csv'), array(
			'incremental' => true,
		));
		$this->_client->addTableColumn($sourceTableId, 'new_column');

		$aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId);

		// and rollback to snapshot
		$this->_client->rollbackTableFromSnapshot($sourceTableId, $snapshotId);

		$tableInfoAfterRollback = $this->_client->getTable($sourceTableId);
		$aliasInfo = $this->_client->getTable($aliasTableId);

		$this->assertEquals($tableInfo['columns'], $tableInfoAfterRollback['columns']);
		$this->assertEquals($tableInfo['columns'], $aliasInfo['columns']);
		$this->assertEquals($tableInfo['primaryKey'], $tableInfoAfterRollback['primaryKey']);
		$this->assertEquals($tableInfo['primaryKey'], $aliasInfo['primaryKey']);
		$this->assertEquals($tableInfo['indexedColumns'], $tableInfoAfterRollback['indexedColumns']);
		$this->assertEquals($tableInfo['indexedColumns'], $aliasInfo['indexedColumns']);
		$this->assertEquals($tableInfo['attributes'], $tableInfoAfterRollback['attributes']);
		$this->assertEmpty($aliasInfo['attributes']);

		$this->assertEquals($tableData, $this->_client->exportTable($sourceTableId));
		$this->assertEquals($tableData, $this->_client->exportTable($aliasTableId));
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