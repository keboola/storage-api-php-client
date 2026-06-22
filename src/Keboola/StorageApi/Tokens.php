<?php
namespace Keboola\StorageApi;

use GuzzleHttp\Psr7\Request as Psr7Request;
use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use RuntimeException;

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

    /**
     * @param null|non-empty-string $applicationToken Manage API token to authorize the
     *     privileged flag. When null, the connection ServiceAccount token is used instead.
     */
    public function createTokenPrivilegedInProtectedDefaultBranch(
        TokenCreateOptions $options,
        ?string $applicationToken = null,
    ): array {
        $authenticator = $applicationToken !== null
            ? new ManageApiTokenAuthenticator($applicationToken)   // X-KBC-ManageApiToken
            : new KeboolaServiceAccountAuthenticator();            // X-Kubernetes-Authorization (connection SA)

        // Authenticators decorate a PSR-7 request; apply to a blank one and lift the
        // header(s) out, since Client works with Guzzle option-arrays, not PSR-7 requests.
        // SA mode reads the projected token file and throws RuntimeException when it is
        // unreadable; wrap it as ClientException so callers get the usual exception type.
        // The manage-token empty-string case throws InvalidArgumentException (a
        // LogicException) and is deliberately not caught here.
        try {
            $elevatedHeaders = $authenticator(new Psr7Request('POST', 'tokens'))->getHeaders();
        } catch (RuntimeException $e) {
            throw new ClientException($e->getMessage(), 0, $e, 'serviceAccountTokenNotReadable');
        }

        $options->setCanManageProtectedDefaultBranch(true);
        $result = $this->client->apiPostJson(
            'tokens',
            $options->toParamsArray(true),
            true,
            [Client::REQUEST_OPTION_HEADERS => $elevatedHeaders],
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
