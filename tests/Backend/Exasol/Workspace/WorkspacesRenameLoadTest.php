<?php

namespace Keboola\Test\Backend\Exasol\Workspace;

class WorkspacesRenameLoadTest extends \Keboola\Test\Backend\Workspaces\WorkspacesRenameLoadTest
{
    public function testDottedDestination(): void
    {
        $this->markTestSkipped('Dotted tables not supported');
    }
}
