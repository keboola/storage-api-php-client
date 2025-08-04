<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class FileWorkspaceTestCase extends WorkspacesTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->deleteAllWorkspaces();
    }

    /**
     * @return string
     */
    protected function resolveFileWorkspaceBackend()
    {
        $tokenInfo = $this->_client->verifyToken();

        switch ($tokenInfo['owner']['fileStorageProvider']) {
            case 'azure':
                return 'abs';
            case 'aws':
            default:
                $this->markTestIncomplete(sprintf('Other file workspace provider than abs not supported'));
        }
    }

    /**
     * @param Workspaces $workspaces
     * @return array
     */
    protected function createFileWorkspace(Workspaces $workspaces)
    {
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $backendType = $this->resolveFileWorkspaceBackend();

        return $workspaces->createWorkspace(
            [
                'backend' => $backendType,
            ],
            true,
        );
    }
}
