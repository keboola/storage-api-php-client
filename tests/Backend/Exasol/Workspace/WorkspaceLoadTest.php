<?php

namespace Keboola\Test\Backend\Exasol\Workspace;

use Keboola\Test\Backend\Workspaces\WorkspacesLoadTest;

class WorkspaceLoadTest extends WorkspacesLoadTest
{
    public function testIncrementalAdditionalColumns()
    {
        $this->markTestSkipped('Needs createTablePrimaryKey');
    }

    public function testIncrementalMissingColumns()
    {
        $this->markTestSkipped('Needs createTablePrimaryKey');
    }

    public function testSecondsFilter()
    {
        $this->markTestSkipped('Needs incremental load');
    }

    public function testDottedDestination()
    {
        $this->markTestSkipped('Dotted tables not supported');
    }
}
