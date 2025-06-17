<?php

namespace Keboola\Test;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\Test\ClientProvider\TestSetupHelper;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\ConsoleOutput;
use Symfony\Component\Console\Output\OutputInterface;

class ClientTestCase extends TestCase
{
    use \PHPUnitRetry\RetryTrait;

    public function getLogger(): ConsoleLogger
    {
        return new ConsoleLogger(new ConsoleOutput(OutputInterface::VERBOSITY_DEBUG));
    }

    /**
     * @return Client
     */
    public function getClient(array $options)
    {
        $options['userAgent'] = $this->buildUserAgentString(
            $options['token'],
            $options['url'],
        );
        if (!array_key_exists('logger', $options)) {
            $options['logger'] = $this->getLogger();
        }
        return new Client($options);
    }

    /**
     * @return BranchAwareClient
     */
    public function getBranchAwareClient($branchId, array $options)
    {
        $options['userAgent'] = $this->buildUserAgentString(
            $options['token'],
            $options['url'],
        );
        return new BranchAwareClient($branchId, $options);
    }

    public function assertManageTokensPresent(): void
    {
        if (!defined('MANAGE_API_TOKEN_ADMIN')) {
            $this->markTestSkipped('Application tokens for tokens tests not configured');
        }
    }

    /**
     * @param array $options
     */
    protected function getManageClient(array $options): \Keboola\ManageApi\Client
    {
        $tokenParts = explode('-', $options['token']);
        $tokenAgentString = '';
        if (count($tokenParts) === 2) {
            $tokenAgentString = sprintf(
                'Token: %s, ',
                $tokenParts[0],
            );
        }

        $testSuiteName = '';
        if (SUITE_NAME) {
            $testSuiteName = sprintf('Suite: %s, ', SUITE_NAME);
        }

        $buildId = '';
        if (TRAVIS_BUILD_ID) {
            $buildId = sprintf('Build id: %s, ', TRAVIS_BUILD_ID);
        }

        $options['userAgent'] = sprintf(
            '%s%sStack: %s, %sTest: %s',
            $buildId,
            $testSuiteName,
            $options['url'],
            $tokenAgentString,
            $this->getTestName(),
        );
        return new \Keboola\ManageApi\Client($options);
    }

    protected function getManageClientForToken(string $token): \Keboola\ManageApi\Client
    {
        return $this->getManageClient($this->getClientOptionsForToken($token));
    }

    protected function getDefaultManageClient(): \Keboola\ManageApi\Client
    {
        return $this->getManageClientForToken(MANAGE_API_TOKEN_ADMIN);
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
     *
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
                    $client->getApiUrl(),
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

    protected function buildUserAgentString(string $token, string $url): string
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
                $tokenParts[1],
            );
        }
        return sprintf(
            '%s%sStack: %s, %sTest: %s',
            $buildId,
            $testSuiteName,
            $url,
            $tokenAgentString,
            $this->getTestName(),
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
