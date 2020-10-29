<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;

class ReadOnlyUserTest extends WorkspacesTestCase
{
    const EXPECTED_ERROR_MESSAGE = 'Your user role "readOnly" does not have access to the resource.';

    public function testWorkspaceRestrictionsForReadOnlyUser()
    {
        $readOnlyClient = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();
        $workspace = $workspaces->getWorkspace($workspace['id']);

        $readOnlyWorkspaces = new Workspaces($readOnlyClient);

        try {
            $readOnlyWorkspaces->createWorkspace();
            $this->fail('Workspace request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame(self::EXPECTED_ERROR_MESSAGE, $e->getMessage());
        }

        try {
            $readOnlyWorkspaces->listWorkspaces();
            $this->fail('Workspace request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame(self::EXPECTED_ERROR_MESSAGE, $e->getMessage());
        }

        try {
            $readOnlyWorkspaces->deleteWorkspace($workspace['id']);
            $this->fail('Workspace request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame(self::EXPECTED_ERROR_MESSAGE, $e->getMessage());
        }

        $this->assertSame($workspace, $workspaces->getWorkspace($workspace['id']));
    }
}
