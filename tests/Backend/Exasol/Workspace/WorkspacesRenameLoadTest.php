<?php

namespace Keboola\Test\Backend\Exasol\Workspace;

class WorkspacesRenameLoadTest extends \Keboola\Test\Backend\Workspaces\WorkspacesRenameLoadTest
{
    public function testDottedDestination()
    {
        $this->markTestSkipped('Dotted tables not supported');
    }
}
