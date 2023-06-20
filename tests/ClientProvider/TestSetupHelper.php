<?php

declare(strict_types=1);

namespace Keboola\Test\ClientProvider;

use Keboola\StorageApi\Client;
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

    /**
     * @param ClientProvider $clientProvider
     * @param ClientProvider::*_BRANCH $devBranchType
     * @param string $userRole
     * @return array{0: Client, 1: Client}
     */
    public function setUpForProtectedDevBranch(
        ClientProvider $clientProvider,
        string $devBranchType,
        string $userRole
    ): array {
        $hasProjectProtectedDefaultbranch = in_array($userRole, ['reviewer', 'developer', 'production-manager']);

        $client = $clientProvider->getDefaultClient();
        if ($hasProjectProtectedDefaultbranch) {
            // default branch is protected, we need privileged client for production cleanup
            $client = $clientProvider->getDefaultClient(['token' => STORAGE_API_DEFAULT_BRANCH_TOKEN]);
        }

        if ($devBranchType === ClientProvider::DEFAULT_BRANCH && $userRole === 'production-manager') {
            $testClient = $clientProvider->getDefaultClient(['token' => STORAGE_API_DEFAULT_BRANCH_TOKEN]);
        } elseif ($devBranchType === ClientProvider::DEV_BRANCH && $userRole === 'developer') {
            $branchName = $clientProvider->getDevBranchName();
            // dev can create & delete branches in production
            $devBranches = new DevBranches($clientProvider->getDefaultClient(['token' => STORAGE_API_DEVELOPER_TOKEN]));
            $this->deleteBranchesByPrefix($devBranches, $branchName);
            $branch = $devBranches->createBranch($branchName);

            // branched client for dev
            $testClient = $clientProvider->getBranchAwareClient($branch['id'], [
                'token' => STORAGE_API_DEVELOPER_TOKEN,
                'url' => STORAGE_API_URL,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]);
        } elseif ($devBranchType === ClientProvider::DEFAULT_BRANCH && $userRole === 'admin') {
            // fallback for normal tests
            $testClient = $clientProvider->createClientForCurrentTest();
        } else {
            throw new \Exception(sprintf('Unknown combination of devBranchType "%s" and userRole "%s"', $devBranchType, $userRole));
        }

        return [$client, $testClient];
    }
}