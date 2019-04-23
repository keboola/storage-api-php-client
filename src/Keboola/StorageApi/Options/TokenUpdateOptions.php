<?php

namespace Keboola\StorageApi\Options;

use Keboola\StorageApi\ClientException;

class TokenUpdateOptions
{
    const BUCKET_PERMISSION_READ = 'read';
    const BUCKET_PERMISSION_WRITE = 'write';
    const ALL_BUCKETS_PERMISSION = 'manage';

    private const ALLOWED_BUCKET_PERMISSIONS = [
        self::BUCKET_PERMISSION_READ,
        self::BUCKET_PERMISSION_WRITE,
    ];

    /** @var string|null */
    private $description;

    /** @var bool|null */
    private $canReadAllFileUploads;

    /** @var int $tokenId */
    private $tokenId;

    /** @var array */
    private $bucketPermissions = [];

    /** @var array|null */
    private $componentAccess = null;

    /**
     * @return string|null
     */
    public function getDescription()
    {
        return $this->description;
    }

    /**
     * @param string $description
     */
    public function setDescription($description)
    {
        $this->description = (string) $description;
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
     * @throws ClientException
     */
    public function addBucketPermission($bucketId, $permission)
    {
        if (!in_array($permission, self::ALLOWED_BUCKET_PERMISSIONS)) {
            throw new ClientException(sprintf(
                "Invalid permission '%s' for bucket '%s'. Allowed permissions are: %s",
                $permission,
                $bucketId,
                implode(', ', self::ALLOWED_BUCKET_PERMISSIONS)
            ));
        }

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
