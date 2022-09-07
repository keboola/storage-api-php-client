<?php

namespace Keboola\StorageApi;

class DevBranches
{
    /** @var Client */
    private $client;

    public function __construct(Client $client)
    {
        if ($client instanceof BranchAwareClient) {
            throw new \LogicException('Cannot use BranchAwareClient for DevBranches');
        }

        $this->client = $client;
    }

    /**
     * @param string $branchName
     * @param string $branchDescription
     */
    public function createBranch($branchName, $branchDescription = '')
    {
        return $this->client->apiPostJson('dev-branches/', ['name' => $branchName, 'description' => $branchDescription]);
    }

    /**
     * @param string $branchName
     * @param string $branchDescription
     */
    public function updateBranch(
        $branchId,
        $branchName = '',
        $branchDescription = ''
    ) {
        $params = [];
        if ($branchName) {
            $params['name'] = $branchName;
        }
        $params['description'] = $branchDescription;
        return $this->client->apiPutJson('dev-branches/' . $branchId, $params);
    }

    /**
     * @param int $branchId
     */
    public function deleteBranch($branchId)
    {
        return $this->client->apiDelete('dev-branches/' . $branchId);
    }

    /**
     * @param int $branchId
     */
    public function getBranch($branchId)
    {
        return $this->client->apiGet('dev-branches/' . $branchId);
    }

    public function listBranches()
    {
        return $this->client->apiGet('dev-branches/');
    }
}
