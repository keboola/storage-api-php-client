<?php

namespace Keboola\StorageApi;

use InvalidArgumentException;
use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\ClientTrait;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\UriInterface;

class BranchAwareGuzzleClient
{
    use ClientTrait;

    private $branchId;

    private Client $client;

    /** @param string|int $branchId */
    public function __construct($branchId, array $config = [])
    {
        if (empty($branchId)) {
            throw new InvalidArgumentException(sprintf('Branch "%s" is not valid.', $branchId));
        }
        $this->branchId = $branchId;
        $this->client = new Client($config);
    }

    /**
     * @param string|UriInterface $uri     URI object or string.
     */
    public function request(string $method, $uri = '', array $options = []): ResponseInterface
    {
        if (strpos($uri, '/v2/storage/branch/default/') === 0) {
            // url already has `branch/default` -> replace with current branch id
            $uri = substr_replace($uri, $this->branchId, strlen('/v2/storage/branch/'), strlen('default'));
        } elseif (strpos($uri, '/v2/storage/') === 0 && strpos($uri, 'jobs/') !== 0) {
            // url without branch -> insert `branch/<id>` into url
            $uri = substr_replace($uri, sprintf('branch/%s/', $this->branchId), strlen('/v2/storage/'), 0);
        }

        return $this->client->request($method, $uri, $options);
    }

    /**
     * @return mixed
     */
    public function getCurrentBranchId()
    {
        return $this->branchId;
    }

    /**
     * @param string|UriInterface $uri     URI object or string.
     */
    public function requestAsync(string $method, $uri, array $options = []): PromiseInterface
    {
        throw new Exception('requestAsync not suppoted');
    }
}
