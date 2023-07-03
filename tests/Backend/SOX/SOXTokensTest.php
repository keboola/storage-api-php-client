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

    public function developerAndReviewerTokensProvider(): Generator
    {
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testTokensVisibility(Client $client): void
    {
        $tokens = new Tokens($client);
        $token = $client->verifyToken();
        // not visible in detail
        $this->assertArrayNotHasKey('token', $token);

        if ($token['canManageTokens']) {
            // if can manage tokens create new non admin token and test if it has hidden token
            $tokens->createToken(
                (new TokenCreateOptions())
                    ->setDescription('Some description')
            );
        }
        $tokenList = $tokens->listTokens();
        foreach ($tokenList as $token) {
            // check all tokens are without decrypted token
            $this->assertArrayNotHasKey('token', $token);
        }
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

    public function testPrivilegedInProtectedMainBranchFailsWithoutAnApplicationTokenWithScope(): void
    {
        $this->assertManageTokensPresent();

        $options = (new TokenCreateOptions())
            ->setDescription('My test token')
            ->setCanReadAllFileUploads(true)
            ->setCanManageBuckets(true)
            ->setCanPurgeTrash(true)
            ->setExpiresIn(360)
            ->addComponentAccess('wr-db');

        try {
            $this->tokens->createTokenPrivilegedInProtectedDefaultBranch($options, '');
            $this->fail('Privileged token request without application token should fail');
        } catch (ClientException $e) {
            $this->assertEquals('Access token must be set', $e->getMessage());
            $this->assertEquals(401, $e->getCode());
        }

        try {
            $this->tokens->createTokenPrivilegedInProtectedDefaultBranch(
                $options,
                MANAGE_API_TOKEN_ADMIN,
            );
            $this->fail('Privileged token request without application token should fail');
        } catch (ClientException $e) {
            $this->assertEquals('You don\'t have access to the resource.', $e->getMessage());
            $this->assertEquals(403, $e->getCode());
        }

        try {
            $this->tokens->createTokenPrivilegedInProtectedDefaultBranch(
                $options,
                MANAGE_API_TOKEN_WITHOUT_SCOPE,
            );
            $this->fail('Privileged token request without application token should fail');
        } catch (ClientException $e) {
            $this->assertEquals('You don\'t have access to the resource.', $e->getMessage());
            $this->assertEquals(403, $e->getCode());
        }

        try {
            $options->setCanManageProtectedDefaultBranch(true);
            // pass in options without using helper method
            $this->tokens->createToken($options);
            $this->fail('Privileged token request without application token should fail');
        } catch (ClientException $e) {
            $this->assertEquals('You don\'t have access to the resource.', $e->getMessage());
            $this->assertEquals(403, $e->getCode());
        }
    }

    public function testPrivilegedInProtectedMainBranchWorksWithApplicationTokenWithCorrectScope(): void
    {
        $this->assertManageTokensPresent();

        $options = (new TokenCreateOptions())
            ->setDescription('My test token')
            ->setCanReadAllFileUploads(true)
            ->setCanManageBuckets(true)
            ->setCanPurgeTrash(true)
            ->setExpiresIn(360)
            ->addComponentAccess('wr-db');

        $token = $this->tokens->createTokenPrivilegedInProtectedDefaultBranch(
            $options,
            MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN,
        );

        $this->assertNotNull($token['expires']);

        $this->assertFalse($token['isMasterToken']);
        $this->assertFalse($token['canManageTokens']);

        $this->assertTrue($token['canManageBuckets']);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertTrue($token['canPurgeTrash']);
        $this->assertTrue($token['canManageProtectedDefaultBranch']);

        $this->assertEquals('My test token', $token['description']);

        $this->assertArrayHasKey('bucketPermissions', $token);
    }

    public function assertManageTokensPresent(): void
    {
        if (!defined('MANAGE_API_TOKEN_ADMIN')
            || !defined('MANAGE_API_TOKEN_WITHOUT_SCOPE')
            || !defined('MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN')
        ) {
            $this->markTestSkipped('Application tokens for tokens tests not configured');
        }
    }

    public function testTokenWithCanCreateJobsFlagCanCreatePriviledgedToken(): void
    {
        // only productionManager can create token with canCreateJobs flag
        $prodManagerClient = $this->getDefaultClient();
        $prodManagerTokens = new Tokens($prodManagerClient);
        $tokenWithCreateJobsFlag = $prodManagerTokens->createToken((new TokenCreateOptions())->setCanCreateJobs(true));
        $clientWithCreateJobsFlag = new Client([
            'token' => $tokenWithCreateJobsFlag['token'],
            'url' => STORAGE_API_URL,
        ]);
        $createJobsFlagTokens = new Tokens($clientWithCreateJobsFlag);
        $priviledgedToken = $createJobsFlagTokens->createTokenPrivilegedInProtectedDefaultBranch(
            (new TokenCreateOptions())
                ->setDescription('My priviledged token')
                ->setCanReadAllFileUploads(true)
                ->setCanManageBuckets(true)
                ->setCanPurgeTrash(true)
                ->addComponentAccess('wr-db'),
            MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN
        );

        $this->assertTrue($priviledgedToken['canManageProtectedDefaultBranch']);
        $this->assertFalse($priviledgedToken['isMasterToken']);
        $this->assertSame('My priviledged token', $priviledgedToken['description']);
    }

    /**
     * @dataProvider developerAndReviewerTokensProvider
     */
    public function testNooneButProdManagerCannotCreateTokenWithCanCreateJobsFlag(Client $client): void
    {
        // only productionManager can create token with canCreateJobs flag
        $tokens = new Tokens($client);
        $createdTokenWithoutCanCreateJobs = $tokens->createToken((new TokenCreateOptions())->setCanCreateJobs(false));
        $this->assertFalse($createdTokenWithoutCanCreateJobs['canCreateJobs']);
        $this->assertFalse($createdTokenWithoutCanCreateJobs['canManageProtectedDefaultBranch']);

        try {
            $tokens->createToken((new TokenCreateOptions())->setCanCreateJobs(true));
            $this->fail('Only productionManager can create token with canCreateJobs flag');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }
    }
}
