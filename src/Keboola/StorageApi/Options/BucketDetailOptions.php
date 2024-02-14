<?php

namespace Keboola\StorageApi\Options;

class BucketDetailOptions
{
    private string $bucketId;

    /** @var string[] $include */
    private array $include;

    /**
     * @param string $bucketId
     * @param string[] $include - available options: columns, metadata, columnMetadata
     */
    public function __construct(string $bucketId, array $include)
    {
        $this->bucketId = $bucketId;
        $this->include = $include;
    }

    public function getBucketId(): string
    {
        return $this->bucketId;
    }

    public function getInclude(): array
    {
        return $this->include;
    }

    public function toParamsArray(): array
    {
        if ($this->include === []) {
            return [];
        }

        return [
            'include' => implode(',', $this->include),
        ];
    }
}
