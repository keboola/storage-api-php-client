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

class DeleteRowsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testTableDeleteRowsAliasShouldBeUpdated()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));
        $this->_client->markTableColumnAsIndexed($tableId, 'city');

        $aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

        $this->_client->deleteTableRows($tableId);

        $tableInfo = $this->_client->getTable($tableId);
        $aliasInfo = $this->_client->getTable($aliasId);

        $this->assertEquals(0, $tableInfo['rowsCount']);
        $this->assertEquals(0, $aliasInfo['rowsCount']);
    }

    public function testDeleteRowsFromAliasShouldNotBeAllowed()
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        $aliasId = $this->_client->createAliasTable($this->getTestBucketId(self::STAGE_OUT), $tableId);

        try {
            $this->_client->deleteTableRows($aliasId);
            $this->fail('Delete rows from alias should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.aliasRowsDeleteNotAllowed', $e->getStringCode());
        }
    }
}
