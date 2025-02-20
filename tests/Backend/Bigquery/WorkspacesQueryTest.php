<?php

namespace Keboola\Test\Backend\Bigquery;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class WorkspacesQueryTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    public function testWorkspaceQuery(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $workspaces = new Workspaces($branchClient);
        $workspace = $this->initTestWorkspace(
            forceRecreate: true,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Only Snowflake workspace is allowed to execute custom query.');
        $workspaces->executeQuery($workspace['id'], 'SHOW TABLES');
    }
}
