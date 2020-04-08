<?php

namespace Keboola\Test\Backend\Synapse;

class LegacyWorkspacesLoadTest extends \Keboola\Test\Backend\Workspaces\LegacyWorkspacesLoadTest
{
    public function testIncrementalAdditionalColumns()
    {
        $this->markTestSkipped('TODO: Implement addTableColumn() method.');
    }

    public function testIncrementalMissingColumns()
    {
        $this->markTestSkipped('TODO: Implement dropTableColumn() method.');
    }
}
