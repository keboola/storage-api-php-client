<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;

class ReadOnlyUserTest extends ParallelWorkspacesTestCase
{
    public function testWorkspaceRestrictionsForReadOnlyUser(): void
    {
        $expectedError = 'You don\'t have access to the resource.';
        $readOnlyClient = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();
        $workspace = $workspaces->getWorkspace($workspace['id']);

        $readOnlyWorkspaces = new Workspaces($readOnlyClient);

        // Try to get workspace detail
        $workspaceDetail = $readOnlyWorkspaces->getWorkspace($workspace['id']);

        // Assert that workspace detail is returned and does not contain sensitive info
        $this->assertArrayHasKey('id', $workspaceDetail);
        $this->assertEquals($workspace['id'], $workspaceDetail['id']);
        $this->assertArrayNotHasKey('password', $workspaceDetail['connection']);

        // listing workspaces should be allowed for readOnly user
        $readOnlyWorkspaces->listWorkspaces();

        try {
            $readOnlyWorkspaces->createWorkspace([], true);
            $this->fail('Workspace request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        try {
            $readOnlyWorkspaces->deleteWorkspace($workspace['id'], [], true);
            $this->fail('Workspace request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        $this->assertSame($workspace, $workspaces->getWorkspace($workspace['id']));
    }
}
