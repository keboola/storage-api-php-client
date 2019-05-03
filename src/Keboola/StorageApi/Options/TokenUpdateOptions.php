<?php

namespace Keboola\StorageApi\Options;

class TokenUpdateOptions extends TokenAbstractOptions
{
    const BUCKET_PERMISSION_READ = 'read';
    const BUCKET_PERMISSION_WRITE = 'write';

    /** @var string|null */
    private $description;

    /** @var bool|null */
    private $canReadAllFileUploads;

    /** @var int $tokenId */
    private $tokenId;

    /** @var array */
    private $bucketPermissions = [];

    /** @var array */
    private $componentAccess = [];

    /**
     * @return string|null
     */
    public function getDescription()
    {
        return $this->description;
    }

    /**
     * @param string $description
     * @return $this
     */
    public function setDescription($description)
    {
        $this->description = (string) $description;
        return $this;
    }

    /**
     * @return bool|null
     */
    public function getCanReadAllFileUploads()
    {
        return $this->canReadAllFileUploads;
    }

    /**
     * @param bool $canReadAll
     * @return $this
     */
    public function setCanReadAllFileUploads($canReadAll)
    {
        $this->canReadAllFileUploads = (bool) $canReadAll;
        return $this;
    }

    /**
     * @return int|null
     */
    public function getTokenId()
    {
        return $this->tokenId;
    }

    /**
     * @param int $tokenId
     * @return $this
     */
    public function setTokenId($tokenId)
    {
        $this->tokenId = (int) $tokenId;
        return $this;
    }

    /**
     * @return array
     */
    public function getBucketPermissions()
    {
        return $this->bucketPermissions;
    }

    /**
     * @param string $bucketId
     * @param string $permission
     * @return $this
     */
    public function addBucketPermission($bucketId, $permission)
    {
        $this->bucketPermissions[$bucketId] = $permission;
        return $this;
    }

    /**
     * @return array|null
     */
    public function getComponentAccess()
    {
        return array_values($this->componentAccess);
    }

    /**
     * @param string $componentId
     * @return $this
     */
    public function addComponentAccess($componentId)
    {
        $this->componentAccess[$componentId] = $componentId;
        return $this;
    }

    /**
     * @return array
     */
    public function toParamsArray()
    {
        $params = [];

        if ($this->getDescription()) {
            $params['description'] = $this->getDescription();
        }

        if ($this->getCanReadAllFileUploads() !== null) {
            $params['canReadAllFileUploads'] = $this->getCanReadAllFileUploads();
        }

        foreach ($this->getBucketPermissions() as $bucketId => $permission) {
            $index = sprintf('bucketPermissions[%s]', $bucketId);
            $params[$index] = $permission;
        }

        foreach ($this->getComponentAccess() as $index => $componentId) {
            $index = sprintf('componentAccess[%s]', $index);
            $params[$index] = $componentId;
        }

        return $params;
    }
}
