<?php

namespace Keboola\StorageApi\Options;

class BucketUpdateOptions
{
    public const REQUEST_COLOR_NO_CHANGE = 'color-no-change';
    /** @var string $bucketId */
    private $bucketId;

    /** @var string $displayName */
    private $displayName;

    /** @var ?string $color */
    private $color = self::REQUEST_COLOR_NO_CHANGE;

    /** @var bool $async */
    private $async;

    public function __construct($bucketId, $displayName, $color = self::REQUEST_COLOR_NO_CHANGE, $async = false)
    {
        $this->bucketId = (string) $bucketId;
        $this->displayName = (string) $displayName;
        $this->color = $color;
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

        if ($this->color !== self::REQUEST_COLOR_NO_CHANGE) {
            $params['color'] = $this->color;
        }

        if ($this->async) {
            $params['async'] = $this->async;
        }

        return $params;
    }
}
