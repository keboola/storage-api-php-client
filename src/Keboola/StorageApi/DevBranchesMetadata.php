<?php

namespace Keboola\StorageApi;

class DevBranchesMetadata
{
    /**
     * @var BranchAwareClient
     */
    private $client;

    public function __construct(BranchAwareClient $client)
    {
        $this->client = $client;
    }
    /**
     * @return array{id: string|numeric, key: string, value: string, timestamp: string}[]
     */
    public function listBranchMetadata()
    {
        /** @var array $response */
        $response = $this->client->apiGet('metadata');
        return $response;
    }

    /**
     * @param array{key: string, value: string}[] $metadata
     * @return array{id: string|numeric, key: string, value: string, timestamp: string}[]
     */
    public function addBranchMetadata(array $metadata)
    {
        /** @var array $response */
        $response = $this->client->apiPostJson('metadata', [
            'metadata' => $metadata,
        ]);
        return $response;
    }

    /**
     * @param int|string $id
     * @return mixed|string
     */
    public function deleteBranchMetadata($id)
    {
        return $this->client->apiDelete("metadata/$id");
    }
}
