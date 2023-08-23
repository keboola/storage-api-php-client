<?php

namespace Keboola\Test;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\Test\ClientProvider\TestSetupHelper;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
    use \PHPUnitRetry\RetryTrait;

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
     * in SOX-related tests, it is considered as production manager client
     * @return Client
     */
    public function getDefaultClient()
    {
        return $this->getClientForToken(STORAGE_API_TOKEN);
    }

    /**
     * @return BranchAwareClient
     */
    public function getBranchAwareDefaultClient($branchId)
    {
        return $this->getBranchAwareClient($branchId, $this->getClientOptionsForToken(STORAGE_API_TOKEN));
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

    protected function getGuestStorageApiClient(): Client
    {
        return $this->getClientForToken(STORAGE_API_GUEST_TOKEN);
    }

    protected function getDefaultBranchStorageApiClient(): Client
    {
        return $this->getClientForToken(STORAGE_API_DEFAULT_BRANCH_TOKEN);
    }

    protected function getDeveloperStorageApiClient(): Client
    {
        return $this->getClientForToken(STORAGE_API_DEVELOPER_TOKEN);
    }

    protected function getReviewerStorageApiClient(): Client
    {
        return $this->getClientForToken(STORAGE_API_REVIEWER_TOKEN);
    }

    protected function getClientBasedOnRole(string $role): Client
    {
        switch ($role) {
            case TestSetupHelper::ROLE_DEVELOPER:
                return $this->getDeveloperStorageApiClient();
            case TestSetupHelper::ROLE_PROD_MANAGER:
                return $this->getDefaultClient();
            default:
                throw new \Exception(sprintf('Unknown role "%s"', $role));
        }
    }

    /**
     * SOX projects require 2 approvals in the process, so we need second person to approve
     */
    protected function getSecondReviewerStorageApiClient(): Client
    {
        return $this->getClientForToken(STORAGE_API_SECOND_REVIEWER_TOKEN);
    }

    protected function getReadOnlyStorageApiClient(): Client
    {
        return $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);
    }

    public function getClientForToken(string $token): Client
    {
        return $this->getClient($this->getClientOptionsForToken($token));
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

    public function getClientOptionsForToken(string $token): array
    {
        return [
            'token' => $token,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ];
    }
}
