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
        return $this->client->apiPost("storage/dev-branches/", ['name' => $branchName]);
    }

    /**
     * @param int $branchId
     */
    public function deleteBranch($branchId)
    {
        return $this->client->apiDelete('storage/dev-branches/' . $branchId);
    }
}
