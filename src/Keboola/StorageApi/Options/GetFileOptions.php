<?php

namespace Keboola\StorageApi\Options;

class GetFileOptions
{
    /**
     * @var bool
     */
    private $federationToken;

    /**
     * @return boolean
     */
    public function getFederationToken()
    {
        return $this->federationToken;
    }

    /**
     * @param $federationToken
     * @return $this
     */
    public function setFederationToken($federationToken): static
    {
        $this->federationToken = (bool) $federationToken;
        return $this;
    }


    public function toArray(): array
    {
        return [
            'federationToken' => $this->getFederationToken(),
        ];
    }
}
