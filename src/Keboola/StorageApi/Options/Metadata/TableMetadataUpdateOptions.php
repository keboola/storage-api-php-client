<?php

namespace Keboola\StorageApi\Options\Metadata;

use Keboola\StorageApi\ClientException;

class TableMetadataUpdateOptions
{
    /** @var string */
    private $tableId;

    /** @var string */
    private $provider;

    /** @var array[] */
    private $tableMetadata;

    /** @var array[] */
    private $columnsMetadata;

    /**
     * @param string $tableId
     * @param string $provider
     * @param array<int, array{key: string, value: string}> $tableMetadata
     * @param array<string, array<int, array{key: string, value: string}>> $columnsMetadata
     */
    public function __construct($tableId, $provider, array $tableMetadata, array $columnsMetadata)
    {
        if (count($tableMetadata) === 0) {
            throw new ClientException("Third argument must be a non-empty array of Metadata objects");
        }
        if (count($columnsMetadata) === 0) {
            throw new ClientException("Fourth argument must be a non-empty array of Metadata objects with columns names as keys");
        }
        $this->tableId = $tableId;
        $this->provider = $provider;
        $this->tableMetadata = $tableMetadata;
        $this->columnsMetadata = $columnsMetadata;
    }

    /**
     * @return string
     */
    public function getTableId()
    {
        return $this->tableId;
    }

    /** @return array */
    public function toParamsArray()
    {
        return [
            "provider" => $this->provider,
            "metadata" => $this->tableMetadata,
            "columnsMetadata" => $this->columnsMetadata,
        ];
    }
}
