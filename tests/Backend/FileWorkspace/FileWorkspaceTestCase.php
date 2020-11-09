<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class FileWorkspaceTestCase extends WorkspacesTestCase
{
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
}
