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
     * Tests: https://github.com/keboola/connection/issues/231
     */
    public function testRowTooBig() {
        $importFile = __DIR__ . '/../../_data/very-long-row.csv';

        try {
            $tableId = $this->_client->createTable(
                $this->getTestBucketId(self::STAGE_IN),
                'rowTooLong',
                new CsvFile($importFile),
                array()
            );
            $this->fail("There was too long an item in the row.");
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('rowTooLarge', $e->getStringCode());
        }
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


    public function testPrimaryKeyDelete()
    {
        $indexColumn = 'city';
        $importFile = __DIR__ . '/../../_data/users.csv';

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
        $importFile =  __DIR__ . '/../../_data/languages-more-columns.csv';

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
        $importFile = __DIR__ . '/../../_data/languages.more-columns.csv';

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
}