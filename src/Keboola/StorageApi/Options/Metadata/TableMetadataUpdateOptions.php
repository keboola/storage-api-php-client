<?php

namespace Keboola\StorageApi\Options\Metadata;

use Keboola\StorageApi\ClientException;

class TableMetadataUpdateOptions
{
    /** @var string */
    private $tableId;

    /** @var string */
    private $provider;

    /** @var array<int, array{key: string, value: string}> */
    private $tableMetadata;

    /** @var array<string|int, array<int, array{key: string, value: string, columnName?: string}>> */
    private $columnsMetadata;

    /**
     * @param string $tableId
     * @param string $provider
     * @param array<int, array{key: string, value: string}>|null $tableMetadata
     * @param array<string|int, array<int, array{key: string, value: string, columnName?: string}>>|null $columnsMetadata
     */
    public function __construct($tableId, $provider, $tableMetadata = null, $columnsMetadata = null)
    {
        if ($tableMetadata !== null && count($tableMetadata) === 0) {
            throw new ClientException('Third argument must be a non-empty array of Metadata objects');
        }
        if ($columnsMetadata !== null && count($columnsMetadata) === 0) {
            throw new ClientException('Fourth argument must be a non-empty array of Metadata objects with columns names as keys');
        }
        if (!$tableMetadata && !$columnsMetadata) {
            throw new ClientException('At least one of the third or fourth argument is required');
        }
        $this->tableId = $tableId;
        $this->provider = $provider;
        $this->tableMetadata = $tableMetadata ?: [];
        $this->columnsMetadata = $columnsMetadata ?: [];

        // backfill column name as array key to value of `columnName` key for all column metadata
        foreach ($this->columnsMetadata as $columnName => &$columnMetadata) {
            foreach ($columnMetadata as &$metadata) {
                if (!array_key_exists('columnName', $metadata)) {
                    $metadata['columnName'] = (string) $columnName;
                }
            }
        }
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
        $data = [
            'provider' => $this->provider,
        ];
        if ($this->tableMetadata) {
            $data['metadata'] = $this->tableMetadata;
        }
        if ($this->columnsMetadata) {
            $data['columnsMetadata'] = $this->columnsMetadata;
        }
        return $data;
    }
}
