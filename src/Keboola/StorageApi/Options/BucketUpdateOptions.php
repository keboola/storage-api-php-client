<?php

namespace Keboola\StorageApi\Options;

class BucketUpdateOptions
{
    /** @var string $bucketId */
    private $bucketId;

    /** @var string $displayName */
    private $displayName;

    public function __construct($bucketId, $displayName)
    {
        $this->bucketId = (string) $bucketId;
        $this->displayName = (string) $displayName;
    }

    /** @return string */
    public function getDisplayName()
    {
        return $this->displayName;
    }

    /** @return string */
    public function getBucketId()
    {
        return $this->bucketId;
    }

    /** @return array */
    public function toParamsArray()
    {
        $params = [];

        if ($this->getDisplayName()) {
            $params['displayName'] = $this->getDisplayName();
        }

        return $params;
    }
}
