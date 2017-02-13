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

class AlterTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    /**
     * Tests: https://github.com/keboola/connection/issues/202
     */
    public function testPrimaryKeyAddTooLong()
    {
        $importFile = __DIR__ . '/../../_data/multiple-columns-pk.csv';

        $primaryKeyColumns = array(
            "Paid_Search_Engine_Account", "Advertiser_ID", "Date", "Paid_Search_Campaign", "Paid_Search_Ad_ID", "Site__DFA"
        );
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'longPK',
            new CsvFile($importFile),
            array()
        );

        try {
            $this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
            $this->fail('create should fail as key will be too long');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.primaryKeyTooLong', $e->getStringCode());
        }
    }


    public function testIndexedColumnsChanges()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';

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

    public function testPrimaryKeyAdd()
    {
        $indexColumn = 'city';
        $primaryKeyColumns = array('id');
        $importFile = __DIR__ . '/../../_data/users.csv';

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

        foreach ($tables as $tableDetail) {
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

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

            $this->assertArrayHasKey('indexedColumns', $tableDetail);
            $this->assertEquals(array_merge(array($indexColumn), $primaryKeyColumns), $tableDetail['indexedColumns']);
        }

        // composite primary key
        $indexColumn = 'iso';
        $primaryKeyColumns = array('Id', 'Name');
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

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

        foreach ($tables as $tableDetail) {
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

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEquals($primaryKeyColumns, $tableDetail['primaryKey']);

            $this->assertArrayHasKey('indexedColumns', $tableDetail);
            $this->assertEquals(array_merge(array($indexColumn), $primaryKeyColumns), $tableDetail['indexedColumns']);
        }

        // existing primary key
        $primaryKeyColumns = array('id');
        $importFile = __DIR__ . '/../../_data/languages.more-columns.csv';

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
}
