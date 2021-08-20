<?php

namespace Keboola\Test\Backend\Exasol\Workspace;

use Keboola\Test\Backend\Workspaces\WorkspacesLoadTest;

class LegacyWorkspaceLoadTest extends WorkspacesLoadTest
{
    public function testDottedDestination()
    {
        $this->markTestSkipped('Dotted tables not supported');
    }
}
