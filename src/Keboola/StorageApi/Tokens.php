<?php
namespace Keboola\StorageApi;

use Keboola\StorageApi\Options\TokenCreateOptions;

class Tokens
{
    /** @var Client */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function listTokens()
    {
        return $this->client->apiGet("tokens");
    }

    /**
     * @return array
     */
    public function getToken($id)
    {
        return $this->client->apiGet("tokens/{$id}");
    }

    /**
     * @return array
     */
    public function createToken(TokenCreateOptions $options)
    {
        return $this->client->apiPost("tokens", $options->toParamsArray());
    }
}
