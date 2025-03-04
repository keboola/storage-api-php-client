<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 12/02/14
 * Time: 12:57
 * To change this template use File | Settings | File Templates.
 */

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

    /**
     * @return array
     */
    public function toArray()
    {
        return [
            'federationToken' => $this->getFederationToken(),
        ];
    }
}
