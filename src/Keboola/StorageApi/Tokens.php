<?php
namespace Keboola\StorageApi;

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
}
