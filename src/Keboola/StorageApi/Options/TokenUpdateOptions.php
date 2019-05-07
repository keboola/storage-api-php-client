<?php

namespace Keboola\StorageApi\Options;

class TokenUpdateOptions extends TokenAbstractOptions
{
    /** @var int $tokenId */
    private $tokenId;

    public function __construct($tokenId)
    {
        $this->tokenId = (int) $tokenId;
    }

    /**
     * @return int|null
     */
    public function getTokenId()
    {
        return $this->tokenId;
    }
}
