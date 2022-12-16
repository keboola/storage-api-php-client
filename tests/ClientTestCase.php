<?php

namespace Keboola\Test;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
//    use \PHPUnitRetry\RetryTrait;

    /**
     * @return Client
     */
    public function getClient(array $options)
    {
        $options['userAgent'] = $this->buildUserAgentString(
            $options['token'],
            $options['url']
        );
        return new Client($options);
    }

    /**
     * @return BranchAwareClient
     */
    public function getBranchAwareClient($branchId, array $options)
    {
        $options['userAgent'] = $this->buildUserAgentString(
            $options['token'],
            $options['url']
        );
        return new BranchAwareClient($branchId, $options);
    }

    /**
     * @return string
     */
    protected function getTestName()
    {
        return get_class($this) . '::' . $this->getName();
    }

    /**
     * @return Client
     */
    public function getDefaultClient()
    {
        return $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * @return BranchAwareClient
     */
    public function getBranchAwareDefaultClient($branchId)
    {
        return $this->getBranchAwareClient($branchId, [
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * @return  \GuzzleHttp\Client
     */
    protected function getGuzzleClientForClient(Client $client)
    {
        return new \GuzzleHttp\Client([
            'base_uri' => $client->getApiUrl(),
            'headers' => [
                'X-StorageApi-Token' => $client->getTokenString(),
                'User-agent' => $this->buildUserAgentString(
                    $client->getTokenString(),
                    $client->getApiUrl()
                ),
            ],
        ]);
    }

    /**
     * @return Client
     */
    protected function getGuestStorageApiClient()
    {
        return $this->getClient([
            'token' => STORAGE_API_GUEST_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * @param string $token
     * @return Client
     */
    public function getClientForToken($token)
    {
        return $this->getClient([
            'token' => $token,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * @param string $token
     * @param string $url
     * @return string
     */
    protected function buildUserAgentString($token, $url)
    {
        $testSuiteName = '';
        if (SUITE_NAME) {
            $testSuiteName = sprintf('Suite: %s, ', SUITE_NAME);
        }

        $buildId = '';
        if (TRAVIS_BUILD_ID) {
            $buildId = sprintf('Build id: %s, ', TRAVIS_BUILD_ID);
        }

        $tokenParts = explode('-', $token);
        $tokenAgentString = '';
        if (count($tokenParts) === 3) {
            // token comes in from of <projectId>-<tokenId>-<hash>
            $tokenAgentString = sprintf(
                'Project: %s, Token: %s, ',
                $tokenParts[0],
                $tokenParts[1]
            );
        }
        return sprintf(
            '%s%sStack: %s, %sTest: %s',
            $buildId,
            $testSuiteName,
            $url,
            $tokenAgentString,
            $this->getTestName()
        );
    }

    /**
     * @param Client $client
     * @return string
     */
    public function getDefaultBackend($client)
    {
        $tokenData = $client->verifyToken();
        return $tokenData['owner']['defaultBackend'];
    }
}
