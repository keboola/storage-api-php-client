<?php

namespace Common;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class WorkspaceDetailTest extends ParallelWorkspacesTestCase
{

    public function testReadOnlyUserCanGetWorkspaceDetail(): void
    {
        // Create workspace as normal user
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->initTestWorkspace();

        // Create a client with a read-only token
        $readOnlyClient = $this->getReadOnlyStorageApiClient(); // Implement this helper to get a read-only token
        $readOnlyWorkspaces = new Workspaces($readOnlyClient);

        // Try to get workspace detail
        $workspaceDetail = $readOnlyWorkspaces->getWorkspace($workspace['id']);

        // Assert that workspace detail is returned and does not contain sensitive info
        $this->assertArrayHasKey('id', $workspaceDetail);
        $this->assertEquals($workspace['id'], $workspaceDetail['id']);
        $this->assertArrayNotHasKey('password', $workspaceDetail['connection']);

        // Cleanup
        $workspaces->deleteWorkspace($workspace['id'], [], true);
    }
}
