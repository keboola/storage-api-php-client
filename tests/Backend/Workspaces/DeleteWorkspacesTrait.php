<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;

trait DeleteWorkspacesTrait
{
    private function deleteAllWorkspaces(): void
    {
        $workspaces = new Workspaces($this->_client);
        foreach ($workspaces->listWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }
    }
}
