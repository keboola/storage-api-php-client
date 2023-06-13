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
        $result = $this->client->apiGet('tokens');
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $id
     * @return array
     */
    public function getToken($id)
    {
        $result = $this->client->apiGet("tokens/{$id}");
        assert(is_array($result));
        return $result;
    }

    /**
     * @return array
     */
    public function createToken(TokenCreateOptions $options)
    {
        $result = $this->client->apiPostJson('tokens', $options->toParamsArray(true));
        assert(is_array($result));
        return $result;
    }

    public function createTokenPrivilegedInProtectedDefaultBranch(TokenCreateOptions $options, string $applicationToken): array
    {
        $headers = [
            Client::REQUEST_OPTION_HEADERS => [
                'X-KBC-ManageApiToken' => $applicationToken,
            ],
        ];
        $options->setCanManageProtectedDefaultBranch(true);
        $result = $this->client->apiPostJson(
            'tokens',
            $options->toParamsArray(true),
            true,
            $headers
        );
        assert(is_array($result));
        return $result;
    }

    /**
     * @return array
     */
    public function updateToken(TokenUpdateOptions $options)
    {
        $result = $this->client->apiPutJson("tokens/{$options->getTokenId()}", $options->toParamsArray(true));
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $id
     * @return void
     */
    public function dropToken($id)
    {
        $this->client->apiDelete("tokens/{$id}");
    }

    /**
     * @param int $id
     * @param string $recipientEmail
     * @param string $message
     * @return void
     */
    public function shareToken($id, $recipientEmail, $message)
    {
        $this->client->apiPostJson("tokens/{$id}/share", [
            'recipientEmail' => $recipientEmail,
            'message' => $message,
        ]);
    }

    /**
     * @param int $id
     * @return array
     */
    public function refreshToken($id)
    {
        $result = $this->client->apiPostJson("tokens/{$id}/refresh");
        assert(is_array($result));
        return $result;
    }
}
