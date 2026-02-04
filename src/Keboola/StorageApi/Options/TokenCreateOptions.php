<?php

namespace Keboola\StorageApi\Options;

class TokenCreateOptions extends TokenAbstractOptions
{
    /** @var int|null */
    private $expiresIn;

    /** @var bool */
    private $canManageBuckets = false;

    private bool $canManageTokens = false;

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

    public function setCanManageTokens(bool $canManageTokens): static
    {
        $this->canManageTokens = $canManageTokens;
        return $this;
    }

    public function canManageTokens(): bool
    {
        return $this->canManageTokens;
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

    /**
     * @param bool $forJson return structure for form-data (false) or for JSON (true)
     */
    public function toParamsArray(bool $forJson = false): array
    {
        $params = parent::toParamsArray($forJson);

        if ($this->getCanManageBuckets()) {
            $params['canManageBuckets'] = true;
        }

        if ($this->canManageTokens()) {
            $params['canManageTokens'] = true;
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
