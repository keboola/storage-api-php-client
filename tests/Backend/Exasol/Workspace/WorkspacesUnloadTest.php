<?php

namespace Keboola\Test\Backend\Exasol\Workspace;

class WorkspacesUnloadTest extends \Keboola\Test\Backend\Workspaces\WorkspacesUnloadTest
{
    public function testTableCloneCaseSensitiveThrowsUserError()
    {
        $this->markTestSkipped('Needs incremental load');
    }

    public function testCopyImport()
    {
        $this->markTestSkipped('Needs incremental load');
    }
}
