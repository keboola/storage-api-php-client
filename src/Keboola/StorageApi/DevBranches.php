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
     * @return array
     */
    public function createBranch($branchName, $branchDescription = '')
    {
        $result = $this->client->apiPostJson('dev-branches/', ['name' => $branchName, 'description' => $branchDescription]);
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $branchId
     * @param string $branchName
     * @param string $branchDescription
     * @return array
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
        $result = $this->client->apiPutJson('dev-branches/' . $branchId, $params);
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $branchId
     * @return void
     */
    public function deleteBranch($branchId)
    {
        $this->client->apiDelete('dev-branches/' . $branchId);
    }

    /**
     * @param int $branchId
     * @return array
     */
    public function getBranch($branchId)
    {
        $result = $this->client->apiGet('dev-branches/' . $branchId);
        assert(is_array($result));
        return $result;
    }

    /**
     * @return array
     */
    public function listBranches()
    {
        $result = $this->client->apiGet('dev-branches/');
        assert(is_array($result));
        return $result;
    }
}
