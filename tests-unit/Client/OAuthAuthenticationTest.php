<?php

namespace Keboola\UnitTest\Client;

use InvalidArgumentException;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class OAuthAuthenticationTest extends TestCase
{
    private function getBasicClientConfig(): array
    {
        return [
            'url' => 'https://connection.keboola.com',
            'token' => 'test-token-12345',
            'logger' => new NullLogger(),
        ];
    }

    public function testDefaultAuthenticationMethodIsToken(): void
    {
        $config = $this->getBasicClientConfig();
        $client = new Client($config);

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $client->getAuthMethod());
    }

    public function testExplicitTokenAuthenticationMethod(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = Client::AUTH_METHOD_TOKEN;

        $client = new Client($config);

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $client->getAuthMethod());
    }

    public function testOAuthAuthenticationMethod(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $client = new Client($config);

        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $client->getAuthMethod());
    }

    public function testInvalidAuthenticationMethodThrowsException(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('authMethod must be "token" or "oauth"');

        $config = $this->getBasicClientConfig();
        $config['authMethod'] = 'invalid_method';

        new Client($config);
    }

    public function testAuthenticationConstants(): void
    {
        $this->assertEquals('token', Client::AUTH_METHOD_TOKEN);
        $this->assertEquals('oauth', Client::AUTH_METHOD_OAUTH);
    }

    public function testBranchAwareClientInheritsAuthMethod(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $branchClient = new BranchAwareClient('test-branch', $config);

        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $branchClient->getAuthMethod());
    }

    public function testBranchAwareClientDefaultBranchPreservesOAuth(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = Client::AUTH_METHOD_OAUTH;

        $branchClient = new BranchAwareClient('test-branch', $config);
        $defaultClient = $branchClient->getDefaultBranchClient();

        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $defaultClient->getAuthMethod());
    }

    public function testBranchAwareClientDefaultBranchPreservesToken(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = Client::AUTH_METHOD_TOKEN;

        $branchClient = new BranchAwareClient('test-branch', $config);
        $defaultClient = $branchClient->getDefaultBranchClient();

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $defaultClient->getAuthMethod());
    }

    public function testGetAuthMethodReturnsCorrectValue(): void
    {
        $tokenConfig = $this->getBasicClientConfig();
        $tokenConfig['authMethod'] = Client::AUTH_METHOD_TOKEN;
        $tokenClient = new Client($tokenConfig);

        $oauthConfig = $this->getBasicClientConfig();
        $oauthConfig['authMethod'] = Client::AUTH_METHOD_OAUTH;
        $oauthClient = new Client($oauthConfig);

        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $tokenClient->getAuthMethod());
        $this->assertEquals(Client::AUTH_METHOD_OAUTH, $oauthClient->getAuthMethod());
    }

    /**
     * Test that authMethod parameter is case sensitive
     */
    public function testAuthMethodIsCaseSensitive(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('authMethod must be "token" or "oauth"');

        $config = $this->getBasicClientConfig();
        $config['authMethod'] = 'TOKEN'; // uppercase should fail

        new Client($config);
    }

    /**
     * Test that empty authMethod falls back to default
     */
    public function testEmptyAuthMethodUsesDefault(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = '';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('authMethod must be "token" or "oauth"');

        new Client($config);
    }

    /**
     * Test that null authMethod uses default
     */
    public function testNullAuthMethodUsesDefault(): void
    {
        $config = $this->getBasicClientConfig();
        $config['authMethod'] = null;

        $client = new Client($config);

        // null should fall back to default (token)
        $this->assertEquals(Client::AUTH_METHOD_TOKEN, $client->getAuthMethod());
    }
}
