<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\Test\StorageApiTestCase;

class SOXTokensTest extends StorageApiTestCase
{
    private function getDefaultBranchTokenId(): int
    {
        [, $tokenId,] = explode('-', STORAGE_API_DEFAULT_BRANCH_TOKEN);
        return (int) $tokenId;
    }

    public function tokensProvider(): Generator
    {
        yield 'nobody can see token (privileged)' => [
            $this->getDefaultBranchStorageApiClient(),
        ];
        yield 'nobody can see token (productionManager)' => [
            $this->getDefaultClient(),
        ];
        yield 'nobody can see token (developer)' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'nobody can see token (reviewer)' => [
            $this->getReviewerStorageApiClient(),
        ];
        yield 'nobody can see token (readOnly)' => [
            $this->getReadOnlyStorageApiClient(),
        ];
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testTokensVisibility(Client $client): void
    {
        $tokens = new Tokens($client);
        $tokenList = $tokens->listTokens();
        foreach ($tokenList as $token) {
            // check all tokens are without decrypted token
            $this->assertArrayNotHasKey('token', $token);
        }

        $token = $client->verifyToken();
        // not visible in detail
        $this->assertArrayNotHasKey('token', $token);
    }

    public function testCannotRefreshCanManageProtectedBranchTokenEvenSelf(): void
    {
        $client = $this->getDefaultBranchStorageApiClient();
        $tokens = new Tokens($client);
        $this->expectExceptionCode(400);
        $this->expectExceptionMessage('Token with canManageProtectedDefaultBranch privilege cannot be refreshed');
        $tokens->refreshToken($this->getDefaultBranchTokenId());
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testCannotShareCanManageProtectedBranchTokenEvenSelf(Client $client): void
    {
        $tokens = new Tokens($client);
        try {
            $tokens->shareToken(
                $this->getDefaultBranchTokenId(),
                'test@devel.keboola.com',
                'hi'
            );
            $this->fail('Nobody can do this.');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testDeleteOwnToken(Client $client): void
    {
        $token = $client->verifyToken();
        $expectCannotCreateToken = false;
        if (!array_key_exists('admin', $token) || $token['admin']['role'] === 'readOnly') {
            $expectCannotCreateToken = true;
        }
        $tokens = new Tokens($client);
        if ($expectCannotCreateToken) {
            $this->expectExceptionMessage('You don\'t have access to the resource.');
            $this->expectExceptionCode(403);
        }
        $newToken = $tokens->createToken(new TokenCreateOptions());

        $this->expectNotToPerformAssertions();
        $newTokenClient = $this->getClientForToken($newToken['token']);
        $tokens = new Tokens($newTokenClient);
        $tokens->dropToken($newToken['id']);
    }
}
