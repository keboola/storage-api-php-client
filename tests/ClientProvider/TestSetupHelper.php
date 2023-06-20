<?php

declare(strict_types=1);

namespace Keboola\Test\ClientProvider;

use Keboola\StorageApi\DevBranches;

class TestSetupHelper
{
    /**
     * @param string $branchPrefix
     */
    public function deleteBranchesByPrefix(DevBranches $devBranches, $branchPrefix): void
    {
        $branchesList = $devBranches->listBranches();
        $branchesCreatedByThisTestMethod = array_filter(
            $branchesList,
            function ($branch) use ($branchPrefix) {
                return strpos($branch['name'], $branchPrefix) === 0;
            },
        );
        foreach ($branchesCreatedByThisTestMethod as $branch) {
            $devBranches->deleteBranch($branch['id']);
        }
    }
}
