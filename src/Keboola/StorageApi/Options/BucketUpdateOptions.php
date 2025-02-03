<?php

namespace Keboola\StorageApi\Options;

class BucketUpdateOptions
{
    /** @var string $bucketId */
    private $bucketId;

    /** @var string $displayName */
    private $displayName;

    /** @var ?string $color */
    private $color = null;

    /** @var bool $deleteColor */
    private $deleteColor = false;

    /** @var bool $async */
    private $async;

    /**
     * @param string $bucketId
     * @param string $displayName
     * @param bool $async
     */
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

    public function getColor(): ?string
    {
        return $this->color;
    }

    public function setColor(?string $color): void
    {
        $this->color = $color;
    }

    private function isDeleteColor(): bool
    {
        return $this->deleteColor;
    }

    public function deleteColor(): void
    {
        $this->deleteColor = true;
    }

    /** @return array */
    public function toParamsArray()
    {
        $params = [];

        if ($this->getDisplayName()) {
            $params['displayName'] = $this->getDisplayName();
        }

        if ($this->isDeleteColor()) {
            $params['color'] = null;
        } elseif ($this->getColor() !== null) {
            $params['color'] = $this->getColor();
        }

        if ($this->async) {
            $params['async'] = $this->async;
        }

        return $params;
    }
}
