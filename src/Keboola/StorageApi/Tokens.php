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

    public function listTokens(): array
    {
        $result = $this->client->apiGet('tokens');
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $id
     */
    public function getToken($id): array
    {
        $result = $this->client->apiGet("tokens/{$id}");
        assert(is_array($result));
        return $result;
    }

    public function createToken(TokenCreateOptions $options): array
    {
        $result = $this->client->apiPostJson('tokens', $options->toParamsArray());
        assert(is_array($result));
        return $result;
    }

    public function updateToken(TokenUpdateOptions $options): array
    {
        $result = $this->client->apiPutJson("tokens/{$options->getTokenId()}", $options->toParamsArray());
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $id
     */
    public function dropToken($id): void
    {
        $this->client->apiDelete("tokens/{$id}");
    }

    /**
     * @param int $id
     * @param string $recipientEmail
     * @param string $message
     */
    public function shareToken($id, $recipientEmail, $message): void
    {
        $this->client->apiPostJson("tokens/{$id}/share", [
            'recipientEmail' => $recipientEmail,
            'message' => $message,
        ]);
    }

    /**
     * @param int $id
     */
    public function refreshToken($id): array
    {
        $result = $this->client->apiPostJson("tokens/{$id}/refresh");
        assert(is_array($result));
        return $result;
    }
}
