<?php

namespace Keboola\StorageApi\Options;

class TokenCreateOptions extends TokenAbstractOptions
{
    /** @var int|null */
    private $expiresIn;

    /** @var bool */
    private $canManageBuckets = false;

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

    public function toParamsArray()
    {
        $params = parent::toParamsArray();

        if ($this->getCanManageBuckets()) {
            $params['canManageBuckets'] = true;
        }

        if ($this->getExpiresIn() !== null) {
            $params['expiresIn'] = $this->getExpiresIn();
        }

        return $params;
    }
}
