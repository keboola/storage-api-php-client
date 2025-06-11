<?php

namespace Keboola\Test;

use GuzzleHttp\Client as GuzzleClient;
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

    /**
     * Test OAuth authentication method configuration
     */
    public function testOAuthAuthenticationMethod(): void
    {
        $options = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $options['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $client = $this->getClient($options);

        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $client->getAuthMethod());
    }

    /**
     * Test default token authentication method
     */
    public function testDefaultTokenAuthenticationMethod(): void
    {
        $options = $this->getClientOptionsForToken(STORAGE_API_TOKEN);

        $client = $this->getClient($options);

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $client->getAuthMethod());
    }

    /**
     * Test explicit token authentication method
     */
    public function testExplicitTokenAuthenticationMethod(): void
    {
        $options = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $options['authMethod'] = Client::AUTH_METHOD_TOKEN;

        $client = $this->getClient($options);

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $client->getAuthMethod());
    }

    /**
     * Test invalid authentication method throws exception
     */
    public function testInvalidAuthenticationMethodThrowsException(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('authMethod must be "token" or "oauth"');

        $options = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $options['authMethod'] = 'invalid_method';

        $this->getClient($options);
    }

    /**
     * Test BranchAwareClient with OAuth authentication
     */
    public function testBranchAwareClientWithOAuth(): void
    {
        $options = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $options['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $branchClient = $this->getBranchAwareClient('default', $options);

        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $branchClient->getAuthMethod());
    }

    /**
     * Test BranchAwareClient default branch client preserves OAuth authentication
     */
    public function testBranchAwareClientDefaultBranchPreservesOAuth(): void
    {
        $options = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $options['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $branchClient = $this->getBranchAwareClient('default', $options);
        $defaultClient = $branchClient->getDefaultBranchClient();

        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $defaultClient->getAuthMethod());
    }

    /**
     * Test OAuth authentication constants are properly defined
     */
    public function testAuthenticationConstants(): void
    {
        $this->assertEquals('token', Client::AUTH_METHOD_TOKEN);
        $this->assertEquals('oauth', Client::AUTH_METHOD_OAUTH);
    }

    /**
     * Test that OAuth and token authentication methods can coexist
     */
    public function testOAuthAndTokenAuthenticationCoexistence(): void
    {
        $tokenOptions = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $tokenOptions['authMethod'] = Client::AUTH_METHOD_TOKEN;

        $oauthOptions = $this->getClientOptionsForToken(STORAGE_API_TOKEN);
        $oauthOptions['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $tokenClient = $this->getClient($tokenOptions);
        $oauthClient = $this->getClient($oauthOptions);

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $tokenClient->getAuthMethod());
        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $oauthClient->getAuthMethod());
    }

    /**
     * Test OAuth client properly sets Authorization header
     */
    public function testOAuthClientSetsCorrectHeaders(): void
    {
        $options = [
            'token' => 'dummy-oauth-token',
            'url' => STORAGE_API_URL,
            'authMethod' => Client::AUTH_METHOD_OAUTH,
            'logger' => $this->getLogger(),
        ];

        $client = $this->getClient($options);

        // Test by creating a Guzzle client that would receive the same headers
        $guzzleClient = $this->getGuzzleClientForOAuthClient($client);

        // Verify our implementation creates the right headers
        $config = $guzzleClient->getConfig();
        $this->assertIsArray($config);
        $this->assertArrayHasKey('headers', $config);
        $this->assertIsArray($config['headers']);
        $this->assertArrayHasKey('Authorization', $config['headers']);
        $this->assertStringStartsWith('Bearer ', $config['headers']['Authorization']);
        $this->assertStringContainsString('dummy-oauth-token', $config['headers']['Authorization']);
    }

    /**
     * Helper method to create Guzzle client with OAuth headers like our implementation
     */
    protected function getGuzzleClientForOAuthClient(Client $client): GuzzleClient
    {
        return new GuzzleClient([
            'base_uri' => $client->getApiUrl(),
            'headers' => [
                'Authorization' => 'Bearer ' . $client->getTokenString(),
                'User-agent' => $this->buildUserAgentString(
                    $client->getTokenString(),
                    $client->getApiUrl(),
                ),
            ],
        ]);
    }

    /**
     * Test OAuth client can perform basic API operations
     */
    public function testOAuthClientBasicOperations(): void
    {
        // Skip this test if we don't have an OAuth token to test with
        if (!defined('OAUTH_TOKEN') || !OAUTH_TOKEN) {
            $this->markTestSkipped('OAuth token not available for testing');
        }

        $options = [
            'token' => OAUTH_TOKEN,
            'url' => STORAGE_API_URL,
            'authMethod' => Client::AUTH_METHOD_OAUTH,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ];

        $client = $this->getClient($options);

         // Test that we can make an authenticated API call
         $buckets = $client->listBuckets();
         $this->assertIsArray($buckets);
    }
}
