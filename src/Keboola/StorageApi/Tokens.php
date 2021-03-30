<?php
namespace Keboola\StorageApi;

use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;

class Tokens
{
    /** @var Client */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * @return array
     */
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

    /**
     * @return array
     */
    public function updateToken(TokenUpdateOptions $options)
    {
        return $this->client->apiPut("tokens/" . $options->getTokenId(), $options->toParamsArray());
    }

    public function dropToken($id)
    {
        $this->client->apiDelete("tokens/" . $id);
    }

    public function shareToken($id, $recipientEmail, $message)
    {
        $this->client->apiPost("tokens/$id/share", [
            'recipientEmail' => $recipientEmail,
            'message' => $message,
        ]);
    }

    /**
     * @return array
     */
    public function refreshToken($id)
    {
        return $this->client->apiPost("tokens/$id/refresh");
    }
}
