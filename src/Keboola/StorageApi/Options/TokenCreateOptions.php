<?php

namespace Keboola\StorageApi\Options;

class TokenCreateOptions extends TokenAbstractOptions
{
    /** @var int|null */
    private $expiresIn;

    /** @var bool */
    private $canManageBuckets = false;

    private bool $canManageProtectedDefaultBranch = false;

    private bool $canCreateJobs = false;

    private bool $canReadAllProjectEvents = false;

    private bool $canManageDevBranches = false;

    /**
     * @return int|null
     */
    public function getExpiresIn()
    {
        return $this->expiresIn;
    }

    /**
     * @param $seconds
     * @return $this
     */
    public function setExpiresIn($seconds): static
    {
        $this->expiresIn = (int) $seconds;
        return $this;
    }

    /**
     * @return bool
     */
    public function getCanManageBuckets()
    {
        return $this->canManageBuckets;
    }

    /**
     * @param bool $allow
     * @return $this
     */
    public function setCanManageBuckets($allow): static
    {
        $this->canManageBuckets = (bool) $allow;
        return $this;
    }

    public function setCanManageProtectedDefaultBranch(bool $canManageProtectedDefaultBranch): static
    {
        $this->canManageProtectedDefaultBranch = $canManageProtectedDefaultBranch;
        return $this;
    }

    public function canManageProtectedDefaultBranch(): bool
    {
        return $this->canManageProtectedDefaultBranch;
    }

    public function setCanCreateJobs(bool $canCreateJobs): static
    {
        $this->canCreateJobs = $canCreateJobs;
        return $this;
    }

    public function canCreateJobs(): bool
    {
        return $this->canCreateJobs;
    }

    public function setCanReadAllProjectEvents(bool $canReadAllProjectEvents = true): self
    {
        $this->canReadAllProjectEvents = $canReadAllProjectEvents;
        return $this;
    }

    public function canReadAllProjectEvents(): bool
    {
        return $this->canReadAllProjectEvents;
    }

    public function setCanManageDevBranches(bool $canManageDevBranches = true): self
    {
        $this->canManageDevBranches = $canManageDevBranches;
        return $this;
    }

    public function canManageDevBranches(): bool
    {
        return $this->canManageDevBranches;
    }

    /**
     * @param bool $forJson return structure for form-data (false) or for JSON (true)
     */
    public function toParamsArray(bool $forJson = false): array
    {
        $params = parent::toParamsArray($forJson);

        if ($this->getCanManageBuckets()) {
            $params['canManageBuckets'] = true;
        }

        if ($this->getExpiresIn() !== null) {
            $params['expiresIn'] = $this->getExpiresIn();
        }

        if ($this->canManageProtectedDefaultBranch()) {
            $params['canManageProtectedDefaultBranch'] = $this->canManageProtectedDefaultBranch();
        }

        if ($this->canCreateJobs()) {
            $params['canCreateJobs'] = $this->canCreateJobs();
        }

        if ($this->canReadAllProjectEvents()) {
            $params['canReadAllProjectEvents'] = $this->canReadAllProjectEvents();
        }

        if ($this->canManageDevBranches()) {
            $params['canManageDevBranches'] = $this->canManageDevBranches();
        }

        return $params;
    }
}
