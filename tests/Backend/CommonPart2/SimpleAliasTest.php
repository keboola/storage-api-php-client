<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\CommonPart2;

use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class SimpleAliasTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testTableAlias()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';

        // create and import data into source table
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id'
            )
        );
        $sourceTable = $this->_client->getTable($sourceTableId);

        // create alias tables
        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'languages-alias-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $firstAliasTableId, 'languages-alias-2');
        $thirdAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $secondAliasTableId, 'languages-alias-3');

        $firstAlias = $this->_client->getTable($firstAliasTableId);
        $secondAlias = $this->_client->getTable($secondAliasTableId);
        $thirdAlias = $this->_client->getTable($thirdAliasTableId);

        // check aliases metadata
        $expectedData = Client::parseCsv(file_get_contents($importFile));
        $this->assertAliasMetadataAndExport(
            $firstAlias,
            $sourceTable,
            $expectedData
        );
        $this->assertAliasMetadataAndExport(
            $secondAlias,
            $firstAlias,
            $expectedData
        );
        $this->assertAliasMetadataAndExport(
            $thirdAlias,
            $secondAlias,
            $expectedData
        );

        // second import into source table
        $this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $sourceTable = $this->_client->getTable($sourceTableId);
        $firstAlias = $this->_client->getTable($firstAliasTableId);
        $secondAlias = $this->_client->getTable($secondAliasTableId);
        $thirdAlias = $this->_client->getTable($thirdAliasTableId);
        $this->assertEquals($sourceTable['lastImportDate'], $firstAlias['lastImportDate']);
        $this->assertEquals($sourceTable['lastImportDate'], $secondAlias['lastImportDate']);
        $this->assertEquals($sourceTable['lastImportDate'], $thirdAlias['lastImportDate']);
        
        // columns auto-create
        $this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../../_data/languages.more-columns.csv'));
        $sourceTable = $this->_client->getTable($sourceTableId);
        $expectedColumns = [
            'id',
            'name',
            'count',
        ];

        $firstAlias = $this->_client->getTable($firstAliasTableId);
        $secondAlias = $this->_client->getTable($secondAliasTableId);
        $thirdAlias = $this->_client->getTable($thirdAliasTableId);
        $this->assertEquals($expectedColumns, $firstAlias['columns'], 'Columns autocreate in alias table');
        $this->assertEquals($expectedColumns, $secondAlias['columns'], 'Columns autocreate in alias table');
        $this->assertEquals($expectedColumns, $thirdAlias['columns'], 'Columns autocreate in alias table');
    }

    private function assertAliasMetadataAndExport($aliasTable, $sourceTable, $expectedData)
    {
        $this->assertNotEmpty($sourceTable['lastImportDate']);
        $this->assertEquals($sourceTable['lastImportDate'], $aliasTable['lastImportDate']);
        $this->assertEquals($sourceTable['lastChangeDate'], $aliasTable['lastChangeDate']);
        $this->assertEquals($sourceTable['columns'], $aliasTable['columns']);
        $this->assertEquals($sourceTable['primaryKey'], $aliasTable['primaryKey']);
        $this->assertTrue($sourceTable['isAliasable']);
        $this->assertNotEmpty($aliasTable['created']);
        $this->assertNotEquals('0000-00-00 00:00:00', $aliasTable['created']);
        $this->assertEquals($sourceTable['rowsCount'], $aliasTable['rowsCount']);
        $this->assertEquals($sourceTable['dataSizeBytes'], $aliasTable['dataSizeBytes']);
        $this->assertTrue($aliasTable['aliasColumnsAutoSync']);
        $this->assertTrue($aliasTable['isAliasable']);

        $this->assertArrayHasKey('sourceTable', $aliasTable);
        $this->assertEquals($sourceTable['id'], $aliasTable['sourceTable']['id'], 'new table linked to source table');

        // alias data preview and async export
        $exporter = new TableExporter($this->_client);
        $downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';

        $this->assertArrayEqualsSorted($expectedData, Client::parseCsv($this->_client->getTableDataPreview($aliasTable['id'])), 'id', 'data are exported from source table');

        $exporter->exportTable($aliasTable['id'], $downloadPath, []);
        $this->assertArrayEqualsSorted($expectedData, Client::parseCsv(file_get_contents($downloadPath)), 'id');
    }

    public function testTableWithAliasShouldNotBeDeletableWithoutForce()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users');

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(400);
        $this->expectExceptionMessageRegExp('/^The table users cannot be deleted because an alias exists./');
        $this->_client->dropTable($sourceTableId);
    }

    public function testTableWithAliasShouldBeForceDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $firstAliasTableId, 'users-2');
        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $secondAliasTableId, 'users-3');

        $this->assertCount(1, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(3, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));

        $this->_client->dropTable($sourceTableId, ['force' => true]);

        $this->assertCount(0, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(0, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));
    }

    public function testAliasWithAliasesShouldBeForceDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $firstAliasTableId, 'users-1-1');
        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $firstAliasTableId, 'users-1-2');
        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $secondAliasTableId, 'users-1-1-1');

        $this->assertCount(1, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(4, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));

        $this->_client->dropTable($secondAliasTableId, ['force' => true]);

        $this->assertCount(1, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(2, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));
    }

    public function testTableAliasFilterModifications()
    {
        // source table
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        // alias table
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users',
            array(
                'aliasFilter' => array(
                    'column' => 'city',
                    'values' => array('PRG'),
                    'operator' => 'eq',
                ),
            )
        );

        $aliasTable = $this->_client->getTable($aliasTableId);

        $this->assertEquals('city', $aliasTable['aliasFilter']['column']);
        $this->assertEquals(array('PRG'), $aliasTable['aliasFilter']['values']);
        $this->assertEquals('eq', $aliasTable['aliasFilter']['operator']);

        $this->assertNull($aliasTable['dataSizeBytes'], 'Filtered alias should have unknown size');
        $this->assertNull($aliasTable['rowsCount'], 'Filtered alias should have unknown rows count');

        $aliasTable = $this->_client->setAliasTableFilter($aliasTableId, array(
            'values' => array('VAN'),
        ));

        $this->assertEquals('city', $aliasTable['aliasFilter']['column']);
        $this->assertEquals(array('VAN'), $aliasTable['aliasFilter']['values']);
        $this->assertEquals('eq', $aliasTable['aliasFilter']['operator']);


        $this->_client->removeAliasTableFilter($aliasTableId);
        $aliasTable = $this->_client->getTable($aliasTableId);

        $this->assertArrayNotHasKey('aliasFilter', $aliasTable);

        $this->_client->dropTable($sourceTableId, ['force' => true]);

        $this->assertCount(0, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(0, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));
    }

    public function testTableAliasUnlink()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';

        // create and import data into source table
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));
        $this->_client->writeTable($sourceTableId, new CsvFile(__DIR__ . '/../../_data/languages.csv'));

        // create alias table
        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId);
        $aliasTable = $this->_client->getTable($aliasTableId);

        $this->assertArrayHasKey('sourceTable', $aliasTable);
        $this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id'], 'new table linked to source table');

        // unlink
        $this->_client->unlinkTable($aliasTableId);

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertArrayNotHasKey('sourceTable', $aliasTable);
        $this->assertEmpty($aliasTable['lastImportDate'], 'Last import date is null');
        $this->assertEquals(0, $aliasTable['dataSizeBytes']);
        $this->assertEquals(0, $aliasTable['rowsCount']);

        // real table cannot be unlinked
        try {
            $this->_client->unlinkTable($aliasTableId);
            $this->fail('Real table should not be unlinked');
        } catch (ClientException $e) {
        }
    }

    public function testAliasColumnWithoutAutoSyncShouldBeDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
            'aliasColumns' => array(
                'city',
                'id',
                'name',
            ),
        ));

        $this->_client->deleteTableColumn($aliasTableId, 'city');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals(array('id', 'name'), $aliasTable['columns']);
    }

    public function testAliasColumnWithoutAutoSyncCanBeAdded()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
            'aliasColumns' => array(
                'id',
                'name',
            ),
        ));

        $this->_client->addTableColumn($aliasTableId, 'city');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals(array('id', 'name', 'city'), $aliasTable['columns']);
    }

    public function testSourceTableColumnDeleteWithAutoSyncAliases()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $firstAliasTableId, 'users-2');

        $this->_client->deleteTableColumn($sourceTableId, 'name', ['force' => true]);

        $expectedColumns = ['id', 'city', 'sex'];
        $this->assertEquals($expectedColumns, $this->_client->getTable($sourceTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);
    }

    public function testSourceTableColumnAddWithAutoSyncAliases()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $firstAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users-1');
        $secondAliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $firstAliasTableId, 'users-2');

        $this->_client->addTableColumn($sourceTableId, 'middleName');

        $expectedColumns = ['id', 'name', 'city', 'sex', 'middleName'];
        $this->assertEquals($expectedColumns, $this->_client->getTable($sourceTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($firstAliasTableId)['columns']);
        $this->assertEquals($expectedColumns, $this->_client->getTable($secondAliasTableId)['columns']);
    }

    public function testAliasColumnsAutoSync()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals(array("id", "name", "city", "sex"), $aliasTable["columns"]);

        $this->_client->addTableColumn($sourceTableId, 'age');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $expectedColumns = array("id", "name", "city", "sex", "age");
        $this->assertEquals($expectedColumns, $aliasTable["columns"]);

        $this->_client->disableAliasTableColumnsAutoSync($aliasTableId);

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertFalse($aliasTable['aliasColumnsAutoSync']);

        $this->_client->addTableColumn($sourceTableId, 'birthDate');
        $this->_client->deleteTableColumn($aliasTableId, 'name');

        $aliasTable = $this->_client->getTable($aliasTableId);

        $expectedColumns = array("id", "city", "sex", "age");
        $this->assertEquals($expectedColumns, $aliasTable["columns"]);

        $data = $this->_client->parseCsv($this->_client->getTableDataPreview($aliasTableId));
        $this->assertEquals($expectedColumns, array_keys(reset($data)));


        $this->_client->enableAliasTableColumnsAutoSync($aliasTableId);
        $aliasTable = $this->_client->getTable($aliasTableId);

        $this->assertEquals(array("id", "name", "city", "sex", "age", "birthDate"), $aliasTable['columns']);
    }

    public function testColumnAssignedToAliasWithAutoSyncShouldNotBeDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $aliasTableId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals(array("id", "name", "city", "sex"), $aliasTable["columns"]);

        $this->_client->addTableColumn($sourceTableId, 'age');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $expectedColumns = array("id", "name", "city", "sex", "age");
        $this->assertEquals($expectedColumns, $aliasTable["columns"]);

        try {
            $this->_client->deleteTableColumn($sourceTableId, 'age');
            $this->fail('Exception should be thrown on table delete when table has aliases');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
        }
    }

    public function testColumnUsedInFilteredAliasShouldNotBeDeletable()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'languages', array(
            'aliasFilter' => array(
                'column' => 'id',
                'values' => array('1'),
            ),
        ));

        try {
            $this->_client->deleteTableColumn($sourceTableId, 'id');
            $this->fail('Exception should be thrown when filtered column is deleted');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
        }
    }

    public function testColumnUsedInFilteredAliasShouldNotBeForceDeletable()
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'languages', array(
            'aliasFilter' => array(
                'column' => 'id',
                'values' => array('1'),
            ),
        ));

        try {
            $this->_client->deleteTableColumn($sourceTableId, 'id', ['force' => true]);
            $this->fail('Exception should be thrown when filtered column is deleted');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
        }
    }

    public function testColumnAssignedToAliasWithoutAutoSyncShouldNotBeDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
            'aliasColumns' => array(
                'city',
                'id',
                'name',
            ),
        ));

        try {
            $this->_client->deleteTableColumn($sourceTableId, 'city');
            $this->fail('Exception should be thrown when referenced column is deleted');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
        }
    }

    public function testColumnAssignedToAliasWithoutAutoSyncShouldNotBeForceDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
            'aliasColumns' => array(
                'city',
                'id',
                'name',
            ),
        ));

        try {
            $this->_client->deleteTableColumn($sourceTableId, 'city', ['force' => true]);
            $this->fail('Exception should be thrown when referenced column is deleted');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
        }
    }

    public function testColumnNotUsedInAnyAliasShouldBeDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
            'aliasColumns' => array(
                'city',
                'id',
            ),
        ));

        $this->_client->deleteTableColumn($sourceTableId, 'name');

        $sourceTable = $this->_client->getTable($sourceTableId);
        $expectedColumns = ["id", "city", "sex"];
        $this->assertEquals($expectedColumns, $sourceTable["columns"]);
    }

    public function testAliasColumns()
    {
        // source table
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $aliasColumns = array(
            'id',
            'city',
        );
        // alias table
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users',
            array(
                'aliasColumns' => $aliasColumns,
            )
        );

        $aliasTable = $this->_client->getTable($aliasTableId);

        $this->assertFalse($aliasTable['aliasColumnsAutoSync']);
        $this->assertEquals($aliasColumns, $aliasTable['columns']);

        $this->_client->addTableColumn($sourceTableId, 'another');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals($aliasTable['columns'], $aliasColumns, 'Column should not be added to alias with auto sync disabled');
    }

    public function testTableWithAliasWithoutAutoSyncShouldBeForceDeletable()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $sourceTableId, 'users', array(
            'aliasColumns' => array(
                'city',
                'id',
                'name',
            ),
        ));

        $this->assertCount(1, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(1, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));

        $this->_client->dropTable($sourceTableId, ['force' => true]);

        $this->assertCount(0, $this->_client->listTables($this->getTestBucketId()));
        $this->assertCount(0, $this->_client->listTables($this->getTestBucketId(self::STAGE_OUT)));
    }

    /**
     * @param $filterOptions
     * @param $expectedResult
     * @dataProvider tableExportFiltersData
     */
    public function testFilteredAliases($filterOptions, $expectedResult)
    {
        // source table
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $aliasParams = [
            'aliasFilter' => [
                'column' => $filterOptions['whereColumn'],
                'operator' => isset($filterOptions['whereOperator']) ? $filterOptions['whereOperator'] : '',
                'values' => $filterOptions['whereValues'],
            ],
        ];

        if (isset($filterOptions['columns'])) {
            $aliasParams['aliasColumns'] = $filterOptions['columns'];
        }
        // alias table
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users',
            $aliasParams
        );

        $data = $this->_client->getTableDataPreview($aliasTableId);
        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

        // async export
        $exporter = new TableExporter($this->_client);
        $downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';
        $exporter->exportTable($aliasTableId, $downloadPath, []);
        $parsedData = Client::parseCsv(file_get_contents($downloadPath), false);
        array_shift($parsedData); // remove header
        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
    }


    /**
     * Test case when alias is filtered but column with filter is not present in alias
     */
    public function testFilteredAliasWithColumnsListed()
    {
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'males',
            array(
                'aliasColumns' => array('id', 'name', 'city'),
                'aliasFilter' => array(
                    'column' => 'sex',
                    'values' => array('male'),
                ),
            )
        );

        $expectedResult = array(
            array(
                "1",
                "martin",
                "PRG",
            ),
            array(
                "3",
                "ondra",
                "VAN",
            ),
            array(
                "4",
                "miro",
                "BRA",
            ),
            array(
                "5",
                "hidden",
                "",
            )
        );

        $data = $this->_client->getTableDataPreview($aliasTableId);
        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header
        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

        // async export
        $exporter = new TableExporter($this->_client);
        $downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';
        $exporter->exportTable($aliasTableId, $downloadPath, []);
        $parsedData = Client::parseCsv(file_get_contents($downloadPath), false);
        array_shift($parsedData); // remove header
        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
    }


    public function testFilterOnFilteredAlias()
    {
        // source table
        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        // alias table
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users',
            array(
                'aliasFilter' => array(
                    'column' => 'city',
                    'values' => array('PRG'),
                ),
            )
        );

        $expectedResult = array(
            array(
                "1",
                "martin",
                "PRG",
                "male"
            )
        );

        $data = $this->_client->getTableDataPreview($aliasTableId, array(
            'whereColumn' => 'sex',
            'whereValues' => array('male'),
        ));
        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header
        $this->assertEquals($expectedResult, $parsedData);

        // async export
        $exporter = new TableExporter($this->_client);
        $downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';
        $exporter->exportTable($aliasTableId, $downloadPath, [
            'whereColumn' => 'sex',
            'whereValues' => array('male'),
        ]);
        $parsedData = Client::parseCsv(file_get_contents($downloadPath), false);
        array_shift($parsedData); // remove header
        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

        $data = $this->_client->getTableDataPreview($aliasTableId, array(
            'whereColumn' => 'city',
            'whereValues' => array('VAN'),
        ));
        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertEmpty($parsedData, 'Export filter should not overload alias filter');

        // async export
        $exporter = new TableExporter($this->_client);
        $downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';
        $exporter->exportTable($aliasTableId, $downloadPath, [
            'whereColumn' => 'city',
            'whereValues' => array('VAN'),
        ]);
        $parsedData = Client::parseCsv(file_get_contents($downloadPath), false);
        array_shift($parsedData); // remove header
        $this->assertEmpty($parsedData);
    }

    public function testAliasingBetweenInAndOutShouldBeAllowed()
    {
        $inTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $inTableId);
        $this->assertNotEmpty($aliasId, 'in -> out');
        $this->_client->dropTable($aliasId);

        $aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $inTableId, 'users-alias');
        $this->assertNotEmpty($aliasId, 'in -> in');
        $this->_client->dropTable($aliasId);

        $outTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_OUT),
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $outTableId, 'users-alias-from-out');
        $this->assertNotEmpty($aliasId, 'out -> out');
        $this->_client->dropTable($aliasId);

        $aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_IN), $outTableId, 'users-alias-from-out');
        $this->assertNotEmpty($aliasId, 'out -> in');
        $this->_client->dropTable($aliasId);
    }

    public function testAliasAutoSyncCannotBeChangedIfDependentAliases()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users'
        );

        $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $aliasTableId,
            'users-2'
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(400);
        $this->expectExceptionMessageRegExp('/^Cannot change auto sync settings - table has dependent aliases$/');
        $this->_client->disableAliasTableColumnsAutoSync($aliasTableId);
    }

    public function testAliasFilterCannotBeChangedIfDependentAliases()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users'
        );

        $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $aliasTableId,
            'users-2'
        );

        $this->expectException(ClientException::class);
        $this->_client->setAliasTableFilter($aliasTableId, [
            'column' => 'name',
            'values' => ['martin'],
        ]);
    }

    public function testAliasingAliasWithoutAutoSyncShouldFail(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users',
            [
                'aliasColumns' => ['id', 'name'],
            ]
        );
        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertFalse($aliasTable['isAliasable']);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Aliasing an advanced alias is not allowed.');
        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $aliasTableId, 'users_alias');
    }

    public function testAliasingAliasWithFilterShouldFail(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $sourceTableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $aliasTableId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $sourceTableId,
            'users',
            [
                'aliasFilter' => [
                    'column' => 'name',
                    'values' => array('foo'),
                    'operator' => 'eq',
                ],
            ]
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Aliasing an advanced alias is not allowed.');
        $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $aliasTableId, 'users_alias');
    }
}
