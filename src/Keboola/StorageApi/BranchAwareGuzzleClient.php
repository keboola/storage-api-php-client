<?php

namespace Keboola\StorageApi;

use \GuzzleHttp\Client;
use GuzzleHttp\ClientTrait;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\UriInterface;

class BranchAwareGuzzleClient
{
    use ClientTrait;

    private $branchId;

    private Client $client;

    public function __construct($branchId, array $config = [])
    {
        if (empty($branchId)) {
            throw new \InvalidArgumentException(sprintf('Branch "%s" is not valid.', $branchId));
        }
        $this->branchId = $branchId;
        $this->client = new Client($config);
    }

    /**
     * @param string|UriInterface $uri     URI object or string.
     */
    public function request(string $method, $uri = '', array $options = []): ResponseInterface
    {
        if (strpos($uri, '/v2/storage/') === 0 && strpos($uri, 'jobs/') !== 0) {
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
        throw new \Exception('requestAsync not suppoted');
    }
}
