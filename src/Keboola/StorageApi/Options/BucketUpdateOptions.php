<?php

namespace Keboola\StorageApi\Options;

class BucketUpdateOptions
{
    /** @var string $bucketId */
    private $bucketId;

    /** @var string $displayName */
    private $displayName;

    /** @var bool $async */
    private $async;

    public function __construct($bucketId, $displayName, $async = false)
    {
        $this->bucketId = (string) $bucketId;
        $this->displayName = (string) $displayName;
        $this->async = $async;
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

        if ($this->async) {
            $params['async'] = $this->async;
        }

        return $params;
    }
}
