<?php

namespace Keboola\StorageApi;

class DevBranches
{
    /** @var Client */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * @param string $branchName
     */
    public function createBranch($branchName)
    {
        return $this->client->apiPost("dev-branches/", ['name' => $branchName]);
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
