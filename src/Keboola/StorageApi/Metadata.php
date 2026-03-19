<?php

namespace Keboola\StorageApi;

use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class Metadata
{
    public const PROVIDER_SYSTEM = 'system';
    public const PROVIDER_STORAGE = 'storage';

    public const BUCKET_METADATA_KEY_ID_BRANCH = 'KBC.createdBy.branch.id';

    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * @param string $bucketId
     * @return array<int, array{id: string, provider:string, key: string, value: string, timestamp: string}> list of metadata
     * @phpstan-impure
     */
    public function listBucketMetadata($bucketId)
    {
        $result = $this->client->apiGet("buckets/{$bucketId}/metadata");
        assert(is_array($result));
        return $result;
    }

    /**
     * @param string $tableId
     * @return array<int, array{id: string, provider:string, key: string, value: string, timestamp: string}> list of metadata
     * @phpstan-impure
     */
    public function listTableMetadata($tableId)
    {
        $result = $this->client->apiGet("tables/{$tableId}/metadata");
        assert(is_array($result));
        return $result;
    }

    /**
     * @param string $columnId  -- ex: "in.c-bucket.table.column"
     * @return array<int, array{id: string, provider:string, key: string, value: string, timestamp: string}> list of metadata
     * @phpstan-impure
     */
    public function listColumnMetadata($columnId)
    {
        $result = $this->client->apiGet("columns/{$columnId}/metadata");
        assert(is_array($result));
        return $result;
    }

    /**
     * @param string $bucketId
     * @param string|int $metadataId
     * @return void
     * @phpstan-impure
     */
    public function deleteBucketMetadata($bucketId, $metadataId)
    {
        $this->client->apiDelete("buckets/{$bucketId}/metadata/{$metadataId}");
    }

    /**
     * @param string $tableId
     * @param string|int $metadataId
     * @return void
     */
    public function deleteTableMetadata($tableId, $metadataId)
    {
        $this->client->apiDelete("tables/{$tableId}/metadata/{$metadataId}");
    }

    /**
     * @param string $columnId
     * @param string|int $metadataId
     * @return void
     */
    public function deleteColumnMetadata($columnId, $metadataId)
    {
        $this->client->apiDelete("columns/{$columnId}/metadata/{$metadataId}");
    }

    /**
     * @param string $bucketId
     * @param string $provider
     * @param array $metadata
     * @return array
     * @throws ClientException
     */
    public function postBucketMetadata($bucketId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException('Third argument must be a non-empty array of metadata objects');
        }
        $result = $this->client->apiPostJson("buckets/{$bucketId}/metadata", [
            'provider' => $provider,
            'metadata' => $metadata,
        ]);
        assert(is_array($result));
        return $result;
    }

    /**
     * @deprecated use self::postTableMetadataWithColumns() instead - beware of a bit different format of response!
     * @see self::postTableMetadataWithColumns()
     *
     * @param string $tableId
     * @param string $provider
     * @param array<int, array{key: string, value: string}> $metadata
     * @return array
     * @throws ClientException
     */
    public function postTableMetadata($tableId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException('Third argument must be a non-empty array of Metadata objects');
        }
        // Keep call with form-data here - call with JSON has different format of response
        $result = $this->client->apiPost("tables/{$tableId}/metadata", [
            'provider' => $provider,
            'metadata' => $metadata,
        ]);
        assert(is_array($result));
        return $result;
    }

    /**
     * @return array
     */
    public function postTableMetadataWithColumns(TableMetadataUpdateOptions $options)
    {
        $result = $this->client->apiPostJson("tables/{$options->getTableId()}/metadata", $options->toParamsArray());
        assert(is_array($result));
        return $result;
    }

    /**
     * @param string $columnId
     * @param string $provider
     * @param array $metadata
     * @return array
     * @throws ClientException
     */
    public function postColumnMetadata($columnId, $provider, $metadata)
    {
        if (!is_array($metadata) || count($metadata) === 0) {
            throw new ClientException('Third argument must be a non-empty array of Metadata objects');
        }
        $result = $this->client->apiPostJson("columns/{$columnId}/metadata", [
            'provider' => $provider,
            'metadata' => $metadata,
        ]);
        assert(is_array($result));
        return $result;
    }
}
