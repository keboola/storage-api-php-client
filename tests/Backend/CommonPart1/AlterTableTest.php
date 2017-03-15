<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class AlterTableTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testTableColumnAdd()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

        $this->_client->addTableColumn($tableId, 'State');

        $detail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('columns', $detail);
        $this->assertContains('State', $detail['columns']);
        $this->assertEquals(array('id', 'name', 'State'), $detail['columns']);

        $importFileWithNewCol = $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $this->_client->writeTable($tableId, new CsvFile($importFileWithNewCol));
        $this->assertLinesEqualsSorted(
            file_get_contents($importFileWithNewCol),
            $this->_client->exportTable($tableId),
            'new column is imported'
        );
    }

    /**
     * @expectedException \Keboola\StorageApi\ClientException
     */
    public function testTableExistingColumnAdd()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));
        $this->_client->addTableColumn($tableId, 'id');
    }

    public function testsTableExistingColumnAddWithDifferentCaseShouldThrowError()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));
        try {
            $this->_client->addTableColumn($tableId, 'ID');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.columnAlreadyExists', $e->getStringCode());
        }
    }

    public function testTableColumnDelete()
    {
        $importFile = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

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
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_REDSHIFT) {
            $this->markTestSkipped('Bug on Redshift backend');
            return;
        }
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => "id,name",
            )
        );

        $detail = $this->_client->getTable($tableId);

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
        $importFile = __DIR__ . '/../../_data/users.csv';

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

        foreach ($tables as $tableDetail) {
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

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEmpty($tableDetail['primaryKey']);

            $this->assertArrayHasKey('indexedColumns', $tableDetail);
            $this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
        }
    }

    /**
     * Tests: https://github.com/keboola/connection/issues/218
     */
    public function testTooManyColumns()
    {
        $importFile = __DIR__ . '/../../_data/many-more-columns.csv';

        try {
            $this->_client->createTable(
                $this->getTestBucketId(self::STAGE_IN),
                'tooManyColumns',
                new CsvFile($importFile),
                array()
            );
            $this->fail("There were 5000 columns man. fail.");
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.tooManyColumns', $e->getStringCode());
        }
    }

    /**
     * Tests: https://github.com/keboola/connection/issues/246
     */
    public function testPrimaryKeyAddWithSameColumnsInDifferentBuckets()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';

        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile($importFile)
        );

        $this->_client->addTableColumn($table1Id, 'new-column');

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_OUT),
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
        $importFile = __DIR__ . '/../../_data/users.csv';

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

        try {
            $this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
        }

        // composite primary key
        $primaryKeyColumns = array('Id', 'Name');
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

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

        try {
            $this->_client->createTablePrimaryKey($tableId, $primaryKeyColumns);
            $this->fail('create should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.primaryKeyDuplicateValues', $e->getStringCode());
        }
    }

    public function testIndexedColumnsCountShouldBeLimited()
    {
        $importFile = __DIR__ . '/../../_data/more-columns.csv';

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

        foreach ($tables as $tableDetail) {
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

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEmpty($tableDetail['primaryKey']);

            $this->assertArrayHasKey('indexedColumns', $tableDetail);
            $this->assertEquals(array($indexColumn), $tableDetail['indexedColumns']);
        }


        // composite primary key
        $indexColumn = 'iso';
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => "Id,Name",
            )
        );

        $this->_client->markTableColumnAsIndexed($tableId, $indexColumn);

        $tableDetail = $this->_client->getTable($tableId);

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

        foreach ($tables as $tableDetail) {
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

    public function testEmptyPrimaryKeyDelete()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $this->_client->removeTablePrimaryKey($tableId);
        $table = $this->_client->getTable($tableId);
        $this->assertEmpty($table['primaryKey']);
    }

    public function testAddInvalidPrimaryKey()
    {
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );
        try {
            $this->_client->createTablePrimaryKey($tableId, ["fakeColumn"]);
            $this->fail("Adding invalid primary key should result in an error");
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals("storage.validation.primaryKey", $e->getStringCode());
        }

        try {
            $this->_client->createTablePrimaryKey($tableId, ["id", "fakeColumn"]);
            $this->fail("Adding invalid primary key should result in an error");
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals("storage.validation.primaryKey", $e->getStringCode());
        }
    }
}
