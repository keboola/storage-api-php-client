<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mysql;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SnapshottingTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testAliasSnapshotCreateShouldNotBeAllowed()
    {
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
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

    public function testTableRollbackFromSnapshot()
    {
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/escaping.csv')
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
        $this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../../_data/escaping.csv'), array(
            'incremental' => true,
        ));
        $this->_client->addTableColumn($sourceTableId, 'new_column');

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId);

        // and rollback to snapshot
        $tableInfoBeforeRollback = $this->_client->getTable($sourceTableId);
        $this->_client->rollbackTableFromSnapshot($sourceTableId, $snapshotId);

        $tableInfoAfterRollback = $this->_client->getTable($sourceTableId);
        $aliasInfo = $this->_client->getTable($aliasTableId);

        $this->assertEquals($tableInfo['columns'], $tableInfoAfterRollback['columns']);
        $this->assertEquals($tableInfo['primaryKey'], $tableInfoAfterRollback['primaryKey']);
        $this->assertEquals($tableInfo['primaryKey'], $aliasInfo['primaryKey']);
        $this->assertEquals($tableInfo['indexedColumns'], $tableInfoAfterRollback['indexedColumns']);
        $this->assertEquals($tableInfo['indexedColumns'], $aliasInfo['indexedColumns']);
        $this->assertEquals($tableInfo['attributes'], $tableInfoAfterRollback['attributes']);


        $this->assertNotEquals($tableInfoBeforeRollback['lastChangeDate'], $tableInfoAfterRollback['lastChangeDate']);
        $this->assertNotEquals($tableInfoBeforeRollback['lastImportDate'], $tableInfoAfterRollback['lastImportDate']);

        $this->assertEquals($tableData, $this->_client->exportTable($sourceTableId));

        $this->assertEmpty($aliasInfo['attributes']);
        $this->assertEquals($tableInfo['columns'], $aliasInfo['columns']);
        $this->assertEquals($tableData, $this->_client->exportTable($aliasTableId));
    }

    public function testRollbackShouldBeDeniedWhenThereAreFilteredAliasesOnColumnsNotIndexedInSnapshot()
    {
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
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
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
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
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
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
