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
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testTableColumnAdd()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

        $this->_client->addTableColumn($tableId, 'State');

        $detail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('columns', $detail);
        $this->assertContains('State', $detail['columns']);
        $this->assertEquals(['id', 'name', 'State'], $detail['columns']);

        $importFileWithNewCol = $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $this->_client->writeTable($tableId, new CsvFile($importFileWithNewCol));
        $this->assertLinesEqualsSorted(
            file_get_contents($importFileWithNewCol),
            $this->_client->getTableDataPreview($tableId),
            'new column is imported'
        );

        $this->_client->addTableColumn($tableId, '_iso');
        $this->_client->addTableColumn($tableId, 'language_name_');

        $detail = $this->_client->getTable($tableId);
        $this->assertEquals(['id', 'name', 'State', 'iso', 'language_name_'], $detail['columns']);
    }

    /**
     * @dataProvider webalizeColumnNameProvider
     * @param $requestedColumnName
     * @param $expectedColumnName
     */
    public function testTableColumnNameShouldBeWebalized($requestedColumnName, $expectedColumnName)
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

        $this->_client->addTableColumn($tableId, $requestedColumnName);

        $table = $this->_client->getTable($tableId);
        $this->assertEquals(['id', 'name', $expectedColumnName], $table['columns']);
    }

    public function webalizeColumnNameProvider()
    {
        return [
            [
                '_abc-def----ghi_',
                'abc_def_ghi_',
            ],
            [
                'žluťoučký    kůň',
                'zlutoucky_kun',
            ],
            [
                'lot__of_____underscores____',
                'lot__of_____underscores____',
            ]
        ];
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

    /**
     * @dataProvider invalidColumnNameProvider
     * @param string $columnName
     */
    public function testAddColumnWithInvalidNameShouldThrowError($columnName)
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));
        try {
            $this->_client->addTableColumn(
                $tableId,
                $columnName
            );
            $this->fail('Column should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.invalidColumnName', $e->getStringCode());
        }
    }

    public function invalidColumnNameProvider()
    {
        return [
            'too long column' => [
                str_repeat('x', 65),
            ],
            'empty column' => [
                '',
            ]
        ];
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

        try {
            $this->_client->deleteTableColumn($tableId, 'name');
            $this->fail('Column should not be deleted.');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeletePrimaryKeyColumn', $e->getStringCode());
        }
    }

    public function testPrimaryKeyAddRequiredParam()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile($importFile),
            []
        );

        $tableDetail = $this->_client->getTable($tableId);
        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEmpty($tableDetail['primaryKey']);

        try {
            $this->_client->createTablePrimaryKey($tableId, []);
            $this->fail('primary key should not be created');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals($e->getStringCode(), 'storage.validation.primaryKey');
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

    public function testPrimaryKeyDelete()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'users',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id'
            )
        );

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

        $tables = array(
            $this->_client->getTable($tableId),
            $this->_client->getTable($aliasTableId),
        );

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEquals(array('id'), $tableDetail['primaryKey']);
        }

        $this->_client->removeTablePrimaryKey($tableId);

        $tables = array(
            $this->_client->getTable($tableId),
            $this->_client->getTable($aliasTableId),
        );

        foreach ($tables as $tableDetail) {
            $this->assertArrayHasKey('primaryKey', $tableDetail);
            $this->assertEmpty($tableDetail['primaryKey']);
        }


        // composite primary key
        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => "Id,Name",
            )
        );

        $tableDetail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEquals(array('Id', 'Name'), $tableDetail['primaryKey']);

        $this->_client->removeTablePrimaryKey($tableId);

        $tableDetail = $this->_client->getTable($tableId);

        $this->assertArrayHasKey('primaryKey', $tableDetail);
        $this->assertEmpty($tableDetail['primaryKey']);

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
