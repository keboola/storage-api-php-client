<?php

namespace Keboola\StorageApi;

use \Keboola\StorageApi\Options\Metadata\Metadatum;

class Metadata
{

    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * @param $bucketId
     * @return mixed|string list of metadata
     */
    public function listBucketMetadata($bucketId)
    {
        return $this->client->apiGet("storage/buckets/{$bucketId}/metadata");
    }

    /**
     * @param $tableId
     * @return mixed|string list of metadata
     */
    public function listTableMetadata($tableId)
    {
        return $this->client->apiGet("storage/tables/{$tableId}/metadata");
    }

    /**
     * @param $columnId  -- ex: "in.c-bucket.table.column"
     * @return mixed|string list of metadata
     */
    public function listColumnMetadata($columnId)
    {
        return $this->client->apiGet("storage/columns/{$columnId}/metadata");
    }

    /**
     * @param $bucketId
     * @param $metadataId
     * @return mixed|string
     */
    public function deleteBucketMetadata($bucketId, $metadataId)
    {
        return $this->client->apiDelete("storage/buckets/{$bucketId}/metadata/{$metadataId}");
    }

    /**
     * @param $tableId
     * @param $metadataId
     * @return mixed|string
     */
    public function deleteTableMetadata($tableId, $metadataId)
    {
        return $this->client->apiDelete("storage/tables/{$tableId}/metadata/{$metadataId}");
    }

    /**
     * @param $columnId
     * @param $metadataId
     * @return mixed|string
     */
    public function deleteColumnMetadata($columnId, $metadataId)
    {
        return $this->client->apiDelete("storage/columns/{$columnId}/metadata/{$metadataId}");
    }

    /**
     * @param $bucketId
     * @param $provider
     * @param array $metadata
     * @return mixed|string
     * @throws ClientException
     */
    public function postBucketMetadata($bucketId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException("Third argument must be a non-empty array of metadata objects");
        }
        return $this->client->apiPost("storage/buckets/{$bucketId}/metadata", array(
            "provider" => $provider,
            "metadata" => $metadata
        ));
    }

    /**
     * @param $tableId
     * @param $provider
     * @param array $metadata
     * @return mixed|string
     * @throws ClientException
     */
    public function postTableMetadata($tableId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException("Third argument must be a non-empty array of Metadata objects");
        }
        return $this->client->apiPost("storage/tables/{$tableId}/metadata", array(
            "provider" => $provider,
            "metadata" => $metadata
        ));
    }

    /**
     * @param $columnId
     * @param $provider
     * @param array $metadata
     * @return mixed|string
     * @throws ClientException
     */
    public function postColumnMetadata($columnId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException("Third argument must be a non-empty array of Metadata objects");
        }
        return $this->client->apiPost("storage/columns/{$columnId}/metadata", array(
            "provider" => $provider,
            "metadata" => $metadata
        ));
    }
}
