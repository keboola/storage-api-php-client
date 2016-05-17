<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Aliases;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;

class CustomSqlAliasTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testAliasedTableDeleteShouldThrowUserError()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_SNOWFLAKE) {
            $this->markTestSkipped('TODO - detect references in snowflake');
            return;
        }

        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );

        $sql = 'SELECT "name" FROM "' . $testBucketId . '"."languages" LIMIT 2';
        $aliasTableId = $this->_client->createRedshiftAliasTable($this->getTestBucketId(self::STAGE_OUT), $sql, 'test');

        try {
            $this->_client->dropTable($sourceTableId);
            $this->fail('Delete should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.dependentObjects', $e->getStringCode());
            $this->assertEquals([strtolower($aliasTableId)], $e->getContextParams()['params']['dependencies']['tables']);
        }
    }

    public function testAliasUnsupportedMethods()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );

        $sql = 'SELECT "name" FROM "' . $testBucketId . '"."languages" LIMIT 2';
        $aliasTableId = $this->_client->createRedshiftAliasTable($this->getTestBucketId(self::STAGE_OUT), $sql, null, $sourceTableId);

        try {
            $this->_client->setAliasTableFilter($aliasTableId, array('values' => array('VAN')));
            $this->fail('Setting of alias filter for redshift backend should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
        }

        try {
            $this->_client->removeAliasTableFilter($aliasTableId, array('values' => array('VAN')));
            $this->fail('Removing of alias filter for redshift backend should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
        }

        try {
            $this->_client->enableAliasTableColumnsAutoSync($aliasTableId);
            $this->fail('Columns syncing of alias filter for redshift backend should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
        }

        try {
            $this->_client->disableAliasTableColumnsAutoSync($aliasTableId);
            $this->fail('Columns syncing of alias filter for redshift backend should fail');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(400, $e->getCode());
        }
    }

    public function testAliasColumnsShouldNotBeSyncedOnSourceTableColumnAdd()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $aliasTableId = $this->_client->createRedshiftAliasTable(
            $aliasBucketId,
            "SELECT \"name\" FROM \"$testBucketId\".\"languages\"",
            null,
            $sourceTableId
        );

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals(array('name'), $aliasTable['columns']);

        $this->_client->addTableColumn($sourceTableId, 'created');
        $sourceTable = $this->_client->getTable($sourceTableId);
        $this->assertEquals(array('id', 'name', 'created'), $sourceTable['columns']);

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals(array('name'), $aliasTable['columns']);
    }

    public function testAliasTimestampColumnShouldBeAllowed()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasTableId = $this->_client->createRedshiftAliasTable(
            $aliasBucketId,
            "SELECT \"id\", \"_timestamp\" FROM \"$testBucketId\".\"languages\"",
            'languages-alias'
        );

        $aliasTable = $this->_client->getTable($aliasTableId);

        $this->assertEquals(array('id'), $aliasTable['columns']);
    }

    public function testAliasCanBeCreatedWithoutTimestampColumn()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasTableId = $this->_client->createRedshiftAliasTable(
            $aliasBucketId,
            "SELECT \"id\" FROM \"$testBucketId\".\"languages\"",
            'languages-alias'
        );


        $data = $this->_client->exportTable($aliasTableId);
        $this->assertNotEmpty($data);

        // sync export is not allowed
        try {
            $this->_client->exportTable($aliasTableId, null, array(
                'changedSince' => '-1 hour'
            ));
            $this->fail('Export should throw exception');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
        }

        // async export is not allowed
        try {
            $this->_client->exportTableAsync($aliasTableId, array(
                'changedSince' => '-1 hour'
            ));
            $this->fail('Export should throw exception');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation', $e->getStringCode());
        }
    }

    public function testInvalidSqlAliases()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $this->_testAliasWithWrongSql($aliasBucketId, "SELECT name AS _name FROM \"$testBucketId\".languages"); // invalid column name
        $this->_testAliasWithWrongSql($aliasBucketId, "SELECT upper(name), upper(name) FROM \"$testBucketId\".languages"); // duplicate upper column
        $this->_testAliasWithWrongSql($aliasBucketId, "SELECT name FROM $testBucketId.languages LIMIT 2");
        $this->_testAliasWithWrongSql($aliasBucketId, "SELECT nonexistent FROM \"$testBucketId\".languages");
        $this->_testAliasWithWrongSql($aliasBucketId, "DELETE FROM \"$testBucketId\".languages");
        $this->_testAliasWithWrongSql($aliasBucketId, "SELECTX FROM \"$testBucketId\".languages");
        $this->_testAliasWithWrongSql($aliasBucketId, "SELECT name FROM $testBucketId.languages LIMIT 2;DELETE FROM \"$testBucketId\".languages");
    }

    public function testErrorOnAliasExportWithInvalidSourceData()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $this->_client->deleteTableRows($sourceTableId);
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasId = $this->_client->createRedshiftAliasTable($aliasBucketId, "SELECT \"name\"::DECIMAL(12,8) as n FROM \"$testBucketId\".\"languages\"", 'number');

        $this->_client->writeTable($sourceTableId, new CsvFile($importFile));

        try {
            $this->_client->exportTableAsync($aliasId);
            $this->fail('export should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.unload', $e->getStringCode());
        }

        $this->_client->dropTable($aliasId);
    }

    private function _testAliasWithWrongSql($aliasBucketId, $sql)
    {
        try {
            $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, uniqid());
            $this->fail('Alias with such sql should fail: ' . $sql);
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('buckets.cannotCreateAliasFromSql', $e->getStringCode());
        }
    }

    public function testAliases()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $sql = "SELECT \"name\" FROM \"$testBucketId\".\"languages\" WHERE \"name\" = 'czech'";
        $aliasTableId = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, null, $sourceTableId);

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertArrayHasKey('selectSql', $aliasTable);
        $this->assertEquals($sql, $aliasTable['selectSql']);
        $this->assertArrayHasKey('isAlias', $aliasTable);
        $this->assertEquals(1, $aliasTable['isAlias']);
        $this->assertEquals($sourceTableId, $aliasTable['sourceTable']['id']);

        $data = $this->_client->exportTable($aliasTableId);
        $parsedData = Client::parseCsv($data, false);
        $this->assertEquals(2, count($parsedData));
        $this->assertEquals(array('czech'), $parsedData[1]);

        $sql2 = "SELECT \"name\" FROM \"$testBucketId\".\"languages\" WHERE \"name\"='english'";
        $this->_client->updateRedshiftAliasTable($aliasTableId, $sql2);

        $data = $this->_client->exportTable($aliasTableId);
        $parsedData = Client::parseCsv($data, false);
        $this->assertEquals(2, count($parsedData));
        $this->assertEquals(array('english'), $parsedData[1]);

        $this->_client->dropTable($aliasTableId);


        // test join
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable(
            $testBucketId,
            'languages2',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $sql = 'SELECT l1."name" AS "name1", l2."name" AS "name2" FROM "'. $testBucketId . '"."languages" l1 LEFT JOIN "' . $testBucketId . '"."languages" l2 ON (l1."id"=l2."id") WHERE l1."name" LIKE \'f%\'';
        $aliasTableId = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql, 'test2');

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEquals($sql, $aliasTable['selectSql']);
        $this->assertArrayNotHasKey('sourceTable', $aliasTable);
        $data = $this->_client->exportTable($aliasTableId);
        $parsedData = Client::parseCsv($data, false);
        $this->assertGreaterThanOrEqual(1, $parsedData);
        $this->assertEquals(array('name1', 'name2'), current($parsedData));

        $this->_client->dropTable($aliasTableId);
    }

    public function testLastImportDateOfAliasIsNotChangedAfterImportToSourceTable()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );

        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasTableId = $this->_client->createRedshiftAliasTable(
            $aliasBucketId,
            "SELECT \"name\" FROM \"$testBucketId\".\"languages\"",
            'languages',
            $sourceTableId
        );

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEmpty($aliasTable['lastChangeDate']);
        $this->assertEmpty($aliasTable['lastImportDate']);

        // import data into source table
        $this->_client->writeTable($sourceTableId, new CsvFile($importFile));

        $aliasTable = $this->_client->getTable($aliasTableId);
        $this->assertEmpty($aliasTable['lastImportDate']);
    }

    public function testAliasAsyncExport()
    {
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $this->_client->createTable(
            $testBucketId,
            'users',
            new CsvFile(__DIR__ . '/../../_data/users.csv')
        );

        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasTableId = $this->_client->createRedshiftAliasTable(
            $aliasBucketId,
            "SELECT \"id\", \"name\" FROM \"$testBucketId\".\"users\"",
            'users'
        );

        $result = $this->_client->exportTableAsync($aliasTableId);
        $file = $this->_client->getFile($result['file']['id']);
        $this->assertNotEmpty(file_get_contents($file['url']));
    }

    public function testColumnUsedInAliasShouldNotBeDeletable()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_SNOWFLAKE) {
            $this->markTestSkipped('TODO - should be fixed on backend');
            return;
        }
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );

        $sql = 'SELECT "name" FROM "' . $testBucketId . '"."languages" LIMIT 2';
        $aliasTableId = $this->_client->createRedshiftAliasTable($this->getTestBucketId(self::STAGE_OUT), $sql, 'test');

        try {
            $this->_client->deleteTableColumn($sourceTableId, 'name');
            $this->fail('Delete should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.dependentObjects', $e->getStringCode());
            $this->assertContains(strtolower($aliasTableId), $e->getMessage());
        }
    }

    public function testAliasShouldNotBeUpdatableIfUsedInAnotherAlias()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_SNOWFLAKE) {
            $this->markTestSkipped('TODO - should be fixed on backend');
            return;
        }
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );

        $sql1 = 'SELECT "name" FROM "' . $testBucketId . '"."languages" LIMIT 2';
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasTable1Id = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql1, 'test1');
        $sql2 = 'SELECT "name" FROM "' . $aliasBucketId . '"."test1"';
        $aliasTable2Id = $this->_client->createRedshiftAliasTable($this->getTestBucketId(self::STAGE_OUT), $sql2, 'test2');

        try {
            $this->_client->updateRedshiftAliasTable($aliasTable1Id, $sql1);
            $this->fail('Update should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.dependentObjects', $e->getStringCode());
            $this->assertContains(strtolower($aliasTable2Id), $e->getMessage());
        }

        $this->_client->dropTable($aliasTable2Id);
    }

    public function testAliasWithDependenciesShouldNotBeDeletable()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_SNOWFLAKE) {
            $this->markTestSkipped('TODO - should be fixed on backend');
            return;
        }
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );

        $sql1 = 'SELECT "name" FROM "' . $testBucketId . '"."languages" LIMIT 2';
        $aliasBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $aliasTable1Id = $this->_client->createRedshiftAliasTable($aliasBucketId, $sql1, 'test1');
        $sql2 = 'SELECT "name" FROM "' . $aliasBucketId . '"."test1"';
        $aliasTable2Id = $this->_client->createRedshiftAliasTable($this->getTestBucketId(self::STAGE_OUT), $sql2, 'test2');

        try {
            $this->_client->dropTable($aliasTable1Id);
            $this->fail('Delete should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.dependentObjects', $e->getStringCode());
            $this->assertContains(strtolower($aliasTable2Id), $e->getMessage());
        }

        $this->_client->dropTable($aliasTable2Id);
    }

}