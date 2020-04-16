<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\CommonPart1\AlterTableTest as CommonAlterTableTest;

class AlterTableTest extends CommonAlterTableTest
{
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
//        // @TODO not implemented yet
//        $this->assertLinesEqualsSorted(
//            file_get_contents($importFileWithNewCol),
//            $this->_client->getTableDataPreview($tableId),
//            'new column is imported'
//        );
    }

    public function testTablePkColumnDelete()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testPrimaryKeyAddRequiredParam()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testPrimaryKeyAddWithSameColumnsInDifferentBuckets()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testPrimaryKeyAddWithDuplicty()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testPrimaryKeyDelete()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testEmptyPrimaryKeyDelete()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testAddInvalidPrimaryKey()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }
}
