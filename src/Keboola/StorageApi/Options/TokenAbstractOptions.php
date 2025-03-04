<?php

namespace Keboola\StorageApi\Options;

abstract class TokenAbstractOptions
{
    const BUCKET_PERMISSION_WRITE = 'write';
    const BUCKET_PERMISSION_READ = 'read';

    /** @var string|null */
    private $description;

    /** @var bool|null */
    private $canReadAllFileUploads;

    /** @var bool|null */
    private $canPurgeTrash;

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
    public function setDescription($description): static
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
    public function setCanReadAllFileUploads($canReadAll): static
    {
        $this->canReadAllFileUploads = (bool) $canReadAll;
        return $this;
    }

    /**
     * @return bool|null
     */
    public function getCanPurgeTrash()
    {
        return $this->canPurgeTrash;
    }

    /**
     * @param bool $canPurgeTrash
     * @return $this
     */
    public function setCanPurgeTrash($canPurgeTrash): static
    {
        $this->canPurgeTrash = (bool) $canPurgeTrash;
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
    public function addBucketPermission($bucketId, $permission): static
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
    public function addComponentAccess($componentId): static
    {
        $this->componentAccess[$componentId] = $componentId;
        return $this;
    }

    /**
     * @param bool $forJson return structure for form-data (false) or for JSON (true)
     * @return array
     */
    public function toParamsArray(bool $forJson = false)
    {
        $params = [];

        if ($this->getDescription()) {
            $params['description'] = $this->getDescription();
        }

        if ($this->getCanReadAllFileUploads() !== null) {
            $params['canReadAllFileUploads'] = $this->getCanReadAllFileUploads();
        }

        if ($this->getCanPurgeTrash() !== null) {
            $params['canPurgeTrash'] = $this->getCanPurgeTrash();
        }

        if ($forJson) {
            if ($this->getBucketPermissions()) {
                $params['bucketPermissions'] = $this->getBucketPermissions();
            }
        } else {
            foreach ($this->getBucketPermissions() as $bucketId => $permission) {
                $index = sprintf('bucketPermissions[%s]', $bucketId);
                $params[$index] = $permission;
            }
        }

        if ($forJson) {
            if ($this->getComponentAccess()) {
                $params['componentAccess'] = $this->getComponentAccess();
            }
        } else {
            foreach ($this->getComponentAccess() as $index => $componentId) {
                $index = sprintf('componentAccess[%s]', $index);
                $params[$index] = $componentId;
            }
        }

        return $params;
    }
}
