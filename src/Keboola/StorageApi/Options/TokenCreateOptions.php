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
    public function setExpiresIn($seconds)
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
    public function setCanManageBuckets($allow)
    {
        $this->canManageBuckets = (bool) $allow;
        return $this;
    }

    public function setCanManageProtectedDefaultBranch(bool $canManageProtectedDefaultBranch): self
    {
        $this->canManageProtectedDefaultBranch = $canManageProtectedDefaultBranch;
        return $this;
    }

    public function canManageProtectedDefaultBranch(): bool
    {
        return $this->canManageProtectedDefaultBranch;
    }

    public function setCanCreateJobs(bool $canCreateJobs): self
    {
        $this->canCreateJobs = $canCreateJobs;
        return $this;
    }

    public function canCreateJobs(): bool
    {
        return $this->canCreateJobs;
    }

    /**
     * @param bool $forJson return structure for form-data (false) or for JSON (true)
     * @return array
     */
    public function toParamsArray(bool $forJson = false)
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

        return $params;
    }
}
