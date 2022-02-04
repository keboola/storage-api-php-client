<?php

namespace Keboola\StorageApi;

use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class Metadata
{
    const PROVIDER_SYSTEM = 'system';
    const PROVIDER_STORAGE = 'storage';

    const BUCKET_METADATA_KEY_ID_BRANCH = 'KBC.createdBy.branch.id';

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
        return $this->client->apiGet("buckets/{$bucketId}/metadata");
    }

    /**
     * @param $tableId
     * @return mixed|string list of metadata
     */
    public function listTableMetadata($tableId)
    {
        return $this->client->apiGet("tables/{$tableId}/metadata");
    }

    /**
     * @param $columnId  -- ex: "in.c-bucket.table.column"
     * @return mixed|string list of metadata
     */
    public function listColumnMetadata($columnId)
    {
        return $this->client->apiGet("columns/{$columnId}/metadata");
    }

    /**
     * @param $bucketId
     * @param $metadataId
     * @return mixed|string
     */
    public function deleteBucketMetadata($bucketId, $metadataId)
    {
        return $this->client->apiDelete("buckets/{$bucketId}/metadata/{$metadataId}");
    }

    /**
     * @param $tableId
     * @param $metadataId
     * @return mixed|string
     */
    public function deleteTableMetadata($tableId, $metadataId)
    {
        return $this->client->apiDelete("tables/{$tableId}/metadata/{$metadataId}");
    }

    /**
     * @param $columnId
     * @param $metadataId
     * @return mixed|string
     */
    public function deleteColumnMetadata($columnId, $metadataId)
    {
        return $this->client->apiDelete("columns/{$columnId}/metadata/{$metadataId}");
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
        return $this->client->apiPost("buckets/{$bucketId}/metadata", array(
            "provider" => $provider,
            "metadata" => $metadata
        ));
    }

    /**
     * @deprecated use self::postTableMetadataWithColumns() instead - beware of a bit different format of response!
     *
     * @param string $tableId
     * @param string $provider
     * @param array<int, array{key: string, value: string}> $metadata
     * @return mixed|string
     * @throws ClientException
     */
    public function postTableMetadata($tableId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException("Third argument must be a non-empty array of Metadata objects");
        }
        return $this->client->apiPost("tables/{$tableId}/metadata", array(
            "provider" => $provider,
            "metadata" => $metadata
        ));
    }

    /**
     * @return mixed|string
     */
    public function postTableMetadataWithColumns(TableMetadataUpdateOptions $options)
    {
        return $this->client->apiPostJson("tables/{$options->getTableId()}/metadata", $options->toParamsArray());
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
        return $this->client->apiPost("columns/{$columnId}/metadata", array(
            "provider" => $provider,
            "metadata" => $metadata
        ));
    }
}
