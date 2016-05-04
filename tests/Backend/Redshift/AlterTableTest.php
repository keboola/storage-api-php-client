<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Redshift;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class AlterTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testRedshiftPrimaryKeyAdd()
    {
        $primaryKeyColumns = array('id');
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/users.csv';


        $tableId = $this->_client->createTable(
            $testBucketId,
            'Users',
            new CsvFile($importFile),
            array()
        );

        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
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
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

        $tableId = $this->_client->createTable(
            $testBucketId,
            'LanGuages',
            new CsvFile($importFile),
            array()
        );

        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
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
        $importFile = __DIR__ . '/../../_data/languages.more-columns.csv';

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
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

    public function testRedshiftPrimaryKeyDelete()
    {
        $indexColumn = 'iso';
        $importFile =  __DIR__ . '/../../_data/languages-more-columns.csv';

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
        $importFile = __DIR__ . '/../../_data/users.csv';

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
        $importFile =  __DIR__ . '/../../_data/languages-more-columns.csv';

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
        $importFile = __DIR__ . '/../../_data/languages.more-columns.csv';

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

}