<?php

namespace Keboola\Test\Backend\Synapse;

class WorkspacesLoadTest extends \Keboola\Test\Backend\Workspaces\WorkspacesLoadTest
{
    public function testIncrementalAdditionalColumns()
    {
        $this->markTestSkipped('TODO: Implement addTableColumn() method.');
    }

    public function testIncrementalMissingColumns()
    {
        $this->markTestSkipped('TODO: Implement dropTableColumn() method.');
    }

    /**
     * @param $exportOptions
     * @param $expectedResult
     * @dataProvider workspaceExportFiltersData
     */
    public function testWorkspaceExportFilters($exportOptions, $expectedResult)
    {
        parent::testWorkspaceExportFilters($exportOptions, $expectedResult);
    }
}
