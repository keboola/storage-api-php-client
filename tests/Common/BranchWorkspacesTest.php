<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\Backend\Workspaces\WorkspacesTest;

class BranchWorkspacesTest extends WorkspacesTest
{

    public function setUp()
    {
        parent::setUp();

        $branches = new DevBranches($this->_client);

        $branchId = null;
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === $this->getTestName()) {
                $branchId = $branch['id'];
            }
        }

        if (!$branchId) {
            $branch = $branches->createBranch($this->getTestName());
            $branchId = $branch['id'];
        }

        $this->workspaceSapiClient = new BranchAwareClient(
            $branchId,
            [
                'token' => $this->initTestToken(),
                'url' => STORAGE_API_URL,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                }
            ]
        );
    }
}
