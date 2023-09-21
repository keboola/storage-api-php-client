<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class SOXTokensTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        foreach ([$this->getDefaultClient(), $this->getDeveloperStorageApiClient()] as $client) {
            $tokensApi = new Tokens($client);
            $tokens = $tokensApi->listTokens();
            foreach ($tokens as $token) {
                if (strpos($token['description'], $this->generateDescriptionForTestObject()) === 0) {
                    try {
                        $tokensApi->dropToken($token['id']);
                    } catch (ClientException $e) {
                        // ignore - it may be not accessible to this client
                    }
                }
            }
        }
    }

    private function getDefaultBranchTokenId(): int
    {
        [, $tokenId,] = explode('-', STORAGE_API_DEFAULT_BRANCH_TOKEN);
        return (int) $tokenId;
    }

    public function tokensProvider(): Generator
    {
        yield 'privileged' => [
            $this->getDefaultBranchStorageApiClient(),
        ];
        yield 'productionManager' => [
            $this->getDefaultClient(),
        ];
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
        yield 'readOnly' => [
            $this->getReadOnlyStorageApiClient(),
        ];
    }

    public function developerAndReviewerClientProvider(): Generator
    {
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
    }

    public function prodManagerClientProvider(): Generator
    {
        yield 'prodManager' => [
            $this->getDefaultClient(),
        ];
    }

    public function privilegedTokenClientProvider(): Generator
    {
        yield 'privileged' => [
            $this->getDefaultBranchStorageApiClient(),
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
            $options = (new TokenCreateOptions())
                ->setDescription($this->generateDescriptionForTestObject());
            if (array_key_exists('admin', $token) && $token['admin']['role'] === 'productionManager') {
                // production manager can only touch tokens with canCreateJobs
                $options->setCanCreateJobs(true);
            }
            $tokens->createToken($options);
        }
        $tokenList = $tokens->listTokens();
        foreach ($tokenList as $token) {
            // check all tokens are without decrypted token
            $this->assertArrayNotHasKey('token', $token);
        }
    }

    /**
     * @dataProvider developerAndReviewerClientProvider
     * @dataProvider prodManagerClientProvider
     * privileged token can refresh self so it's not testes since it would broke test suite
     */
    public function testCannotRefreshCanManageProtectedBranchTokenEvenSelf(Client $client): void
    {
        $tokens = new Tokens($client);
        $this->expectExceptionCode(403);
        $this->expectExceptionMessage('You don\'t have access to the resource.');
        // getDefaultBranchTokenId = privileged token
        $tokens->refreshToken($this->getDefaultBranchTokenId());
    }

    /**
     * @dataProvider provideTokenRefreshFailingParams
     * in sox project nobody can refresh token which is not self
     */
    public function testRefreshTokenFails(Client $client, callable $setupToken): void
    {
        $tokens = new Tokens($client);
        $tokenId = $setupToken($this->generateDescriptionForTestObject());

        sleep(1);

        $this->expectExceptionCode(403);
        $this->expectExceptionMessage('You don\'t have access to the resource.');
        $tokens->refreshToken($tokenId);
    }

    /**
     * @dataProvider provideTokenRefreshPassingParams
     * in sox project nobody can refresh token which is not self
     */
    public function testRefreshTokenSucceeds(Client $client, callable $setupToken): void
    {
        $tokens = new Tokens($client);
        $tokenId = $setupToken($this->generateDescriptionForTestObject());

        sleep(1);

        $token = $tokens->refreshToken($tokenId);
        $this->assertArrayHasKey('token', $token);
        $this->assertMatchesRegularExpression('~^[0-9]+-[0-9]+-[0-9A-Za-z]+$~', $token['token']);
    }

    public function testWhoCanRefreshTokenWithCanCreateJobs(): void
    {
        $prodManTokensApi = new Tokens($this->getDefaultClient());
        $token = $prodManTokensApi->createToken(
            (new TokenCreateOptions())
                ->setCanCreateJobs(true)
                ->setDescription($this->generateDescriptionForTestObject()),
        );

        $refreshedToken = $prodManTokensApi->refreshToken($token['id']);
        $this->assertSame($token['id'], $refreshedToken['id']);
        $this->assertNotSame($token['token'], $refreshedToken['token']);

        $canCreateJobs = $this->getClientForToken($refreshedToken['token']);
        $this->assertTrue($canCreateJobs->verifyToken()['canCreateJobs']);
        $canCreateJobsTokensApi = new Tokens($canCreateJobs);
        $refreshedTokenSecond = $canCreateJobsTokensApi->refreshToken($refreshedToken['id']);

        $this->assertSame($refreshedToken['id'], $refreshedTokenSecond['id']);
        $this->assertNotSame($refreshedToken['token'], $refreshedTokenSecond['token']);

        foreach ([$this->getDeveloperStorageApiClient(), $this->getReviewerStorageApiClient()] as $nonPmClient) {
            $nonPmTokensApi = new Tokens($nonPmClient);

            try {
                $nonPmTokensApi->refreshToken($refreshedTokenSecond['id']);
                $this->fail('Developer should not be able to refresh token with canCreateJobs');
            } catch (ClientException $e) {
                $this->assertSame(403, $e->getCode());
                $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
            }

            $noCanCreateJobsToken = $nonPmTokensApi->createToken(
                (new TokenCreateOptions())
                    ->setCanCreateJobs(false)
                    ->setDescription($this->generateDescriptionForTestObject()),
            );
            $noCanCreateJobsTokenRefreshed = $nonPmTokensApi->refreshToken($noCanCreateJobsToken['id']);

            $this->assertSame($noCanCreateJobsToken['id'], $noCanCreateJobsTokenRefreshed['id']);
            $this->assertNotSame($noCanCreateJobsToken['token'], $noCanCreateJobsTokenRefreshed['token']);
            try {
                $prodManTokensApi->refreshToken($noCanCreateJobsToken['id']);
                $this->fail('Prod manager should not be able to refresh token without canCreateJobs');
            } catch (ClientException $e) {
                $this->assertSame(403, $e->getCode());
                $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
            }
        }
    }

    public function testTokenSelfRefresh(): void
    {
        $tokens = new Tokens($this->getDefaultClient());
        $token = $tokens->createToken(
            // user is PM and can't create any other tokens than canCreateJobs
            (new TokenCreateOptions())
                ->setDescription($this->generateDescriptionForTestObject())
                ->setCanCreateJobs(true),
        );

        $client = $this->getClientForToken($token['token']);
        $refreshedToken = (new Tokens($client))->refreshToken($token['id']);
        $this->assertSame($token['id'], $refreshedToken['id']);
        $this->assertNotSame($token['token'], $refreshedToken['token']);
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
        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject());
        if (array_key_exists('admin', $token) && $token['admin']['role'] === 'productionManager') {
            $options->setCanCreateJobs(true);
        }
        $newToken = $tokens->createToken(
            $options,
        );

        $this->expectNotToPerformAssertions();
        $newTokenClient = $this->getClientForToken($newToken['token']);
        $tokens = new Tokens($newTokenClient);
        $tokens->dropToken($newToken['id']);
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testDeleteTokenWithDifferentCanCreateJobs(Client $client): void
    {
        $token = $client->verifyToken();
        $adminRole = null;
        if (array_key_exists('admin', $token)) {
            $adminRole = $token['admin']['role'];
        }

        if ($adminRole === 'productionManager' || $adminRole === null) {
            $tokenCreatingClient = $this->getDeveloperStorageApiClient();
        } else {
            $tokenCreatingClient = $this->getDefaultClient();
        }
        $tokenCreationApi = new Tokens($tokenCreatingClient);

        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject());
        if (!in_array($adminRole, [null, 'productionManager'], true)) {
            $options->setCanCreateJobs(true);
        }

        $newToken = $tokenCreationApi->createToken(
            $options,
        );

        $tokens = new Tokens($client);
        $this->expectExceptionMessage('You don\'t have access to the resource');
        $this->expectExceptionCode(403);
        $this->expectException(ClientException::class);
        $tokens->dropToken($newToken['id']);
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testDeleteTokenWithSameCanCreateJobs(Client $client): void
    {
        $token = $client->verifyToken();
        $adminRole = null;
        if (array_key_exists('admin', $token)) {
            $adminRole = $token['admin']['role'];
        }

        if ($adminRole === 'productionManager' || $adminRole === null) {
            $tokenCreatingClient = $this->getDefaultClient();
        } else {
            $tokenCreatingClient = $this->getDeveloperStorageApiClient();
        }
        $tokenCreationApi = new Tokens($tokenCreatingClient);

        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject());
        if (in_array($adminRole, [null, 'productionManager'], true)) {
            $options->setCanCreateJobs(true);
        }

        $newToken = $tokenCreationApi->createToken(
            $options,
        );

        $tokens = new Tokens($client);
        if ($adminRole === 'readOnly') {
            $this->expectExceptionMessage('You don\'t have access to the resource');
            $this->expectExceptionCode(403);
        }
        $tokens->dropToken($newToken['id']);
        $this->expectNotToPerformAssertions();
    }

    public function testPrivilegedInProtectedMainBranchFailsWithoutAnApplicationTokenWithScope(): void
    {
        $this->assertManageTokensPresent();

        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject())
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
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->assertManageTokensPresent();

        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject())
            ->setCanReadAllFileUploads(true)
            ->setCanManageBuckets(true)
            ->setCanPurgeTrash(true)
            ->setExpiresIn(360)
            ->addComponentAccess('wr-db');

        [$manageTokenId, $_] = explode('-', MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN);

        // check that the application token id is logged in event
        $qb = new EventsQueryBuilder();
        $qb->setEvent('storage.tokenCreated');
        $qb->setRunId($runId);
        $query = $qb->generateQuery();

        $apiCall = fn() => $this->_client->listEvents([
            'sinceId' => $this->lastEventId,
            'limit' => 1,
            'q' => $query,
        ]);
        $assertContainsIdManageToken = function ($events) use ($manageTokenId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'idManageToken' => (int) $manageTokenId,
                'canManageProtectedDefaultBranch' => true,
            ], $events[0]['params']);
        };

        $token = $this->tokens->createTokenPrivilegedInProtectedDefaultBranch(
            $options,
            MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN,
        );

        $this->retryWithCallback($apiCall, $assertContainsIdManageToken);

        $this->assertNotNull($token['expires']);

        $this->assertFalse($token['isMasterToken']);
        $this->assertFalse($token['canManageTokens']);

        $this->assertTrue($token['canManageBuckets']);
        $this->assertTrue($token['canReadAllFileUploads']);
        $this->assertTrue($token['canPurgeTrash']);
        $this->assertTrue($token['canManageProtectedDefaultBranch']);

        $this->assertEquals($this->generateDescriptionForTestObject(), $token['description']);

        $this->assertArrayHasKey('bucketPermissions', $token);
    }

    public function testPrivilegedTokenCanRefreshSelf(): void
    {
        $this->assertManageTokensPresent();

        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject())
            ->setCanReadAllFileUploads(true)
            ->setCanManageBuckets(true)
            ->setCanPurgeTrash(true)
            ->setExpiresIn(360)
            ->addComponentAccess('wr-db');

        $token = $this->tokens->createTokenPrivilegedInProtectedDefaultBranch(
            $options,
            MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN,
        );

        $client = $this->getClientForToken($token['token']);
        $refreshedToken = (new Tokens($client))->refreshToken($token['id']);
        $this->assertSame($token['id'], $refreshedToken['id']);
        $this->assertNotSame($token['token'], $refreshedToken['token']);
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
        $tokenWithCreateJobsFlag = $prodManagerTokens->createToken(
            (new TokenCreateOptions())
                ->setCanCreateJobs(true)
                ->setDescription($this->generateDescriptionForTestObject() . '-can create jobs'),
        );
        $clientWithCreateJobsFlag = new Client([
            'token' => $tokenWithCreateJobsFlag['token'],
            'url' => STORAGE_API_URL,
        ]);
        $createJobsFlagTokens = new Tokens($clientWithCreateJobsFlag);
        $priviledgedToken = $createJobsFlagTokens->createTokenPrivilegedInProtectedDefaultBranch(
            (new TokenCreateOptions())
                ->setDescription($this->generateDescriptionForTestObject() . '-privileged')
                ->setCanReadAllFileUploads(true)
                ->setCanManageBuckets(true)
                ->setCanPurgeTrash(true)
                ->addComponentAccess('wr-db'),
            MANAGE_API_TOKEN_WITH_CREATE_PROTECTED_DEFAULT_BRANCH_TOKEN
        );

        $this->assertTrue($priviledgedToken['canManageProtectedDefaultBranch']);
        $this->assertFalse($priviledgedToken['isMasterToken']);
        $this->assertSame($this->generateDescriptionForTestObject() . '-privileged', $priviledgedToken['description']);
    }

    /**
     * @dataProvider developerAndReviewerClientProvider
     */
    public function testNooneButProdManagerCannotCreateTokenWithCanCreateJobsFlag(Client $client): void
    {
        // only productionManager can create token with canCreateJobs flag
        $tokens = new Tokens($client);
        $createdTokenWithoutCanCreateJobs = $tokens->createToken(
            (new TokenCreateOptions())
                ->setCanCreateJobs(false)
                ->setDescription($this->generateDescriptionForTestObject()),
        );
        $this->assertFalse($createdTokenWithoutCanCreateJobs['canCreateJobs']);
        $this->assertFalse($createdTokenWithoutCanCreateJobs['canManageProtectedDefaultBranch']);

        try {
            $tokens->createToken(
                (new TokenCreateOptions())
                    ->setCanCreateJobs(true)
                    ->setDescription($this->generateDescriptionForTestObject()),
            );
            $this->fail('Only productionManager can create token with canCreateJobs flag');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
        }
    }

    /**
     * @return iterable<array{Client, callable(string): string}>
     */
    public function provideTokenRefreshFailingParams(): iterable
    {
        yield 'developer cannot refresh can create jobs token' => [
            $this->getDeveloperStorageApiClient(),
            function ($description) {
                $prodManagerClient = $this->getDefaultClient();
                $prodManagerTokens = new Tokens($prodManagerClient);
                $token = $prodManagerTokens->createToken((new TokenCreateOptions())->setCanCreateJobs(true)->setDescription($description));
                return $token['id'];
            },
        ];
        yield 'reviewer cannot refresh can create jobs token' => [
            $this->getDeveloperStorageApiClient(),
            function ($description) {
                $prodManagerClient = $this->getDefaultClient();
                $prodManagerTokens = new Tokens($prodManagerClient);
                $token = $prodManagerTokens->createToken((new TokenCreateOptions())->setCanCreateJobs(true)->setDescription($description));
                return $token['id'];
            },
        ];
        yield 'prodManager cannot refresh normal token' => [
            $this->getDefaultClient(),
            function ($description) {
                $devClient = $this->getDeveloperStorageApiClient();
                $devTokens = new Tokens($devClient);
                $token = $devTokens->createToken((new TokenCreateOptions())->setDescription($description));
                return $token['id'];
            },
        ];
    }

    /**
     * @return iterable<array{Client, callable(string): string}>
     */
    public function provideTokenRefreshPassingParams(): iterable
    {
        yield 'developer can refresh normal token' => [
            $this->getDeveloperStorageApiClient(),
            function ($description) {
                $prodManagerClient = $this->getDeveloperStorageApiClient();
                $prodManagerTokens = new Tokens($prodManagerClient);
                $token = $prodManagerTokens->createToken((new TokenCreateOptions())->setCanCreateJobs(false)->setDescription($description));
                return $token['id'];
            },
        ];
        yield 'reviewer can refresh normal token' => [
            $this->getDeveloperStorageApiClient(),
            function ($description) {
                $prodManagerClient = $this->getDeveloperStorageApiClient();
                $prodManagerTokens = new Tokens($prodManagerClient);
                $token = $prodManagerTokens->createToken((new TokenCreateOptions())->setCanCreateJobs(false)->setDescription($description));
                return $token['id'];
            },
        ];
        yield 'prodManager can refresh can create jobs token' => [
            $this->getDefaultClient(),
            function ($description) {
                $devClient = $this->getDefaultClient();
                $devTokens = new Tokens($devClient);
                $token = $devTokens->createToken((new TokenCreateOptions())->setCanCreateJobs(true)->setDescription($description));
                return $token['id'];
            },
        ];
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testWhoCannotCreateToken(Client $client): void
    {
        $token = $client->verifyToken();
        $role = null;
        if (array_key_exists('admin', $token)) {
            $role = $token['admin']['role'];
        }
        $tokensApi = new Tokens($client);
        $options = (new TokenCreateOptions())
            ->setDescription($this->generateDescriptionForTestObject());
        if ($role !== 'productionManager') {
            $options->setCanCreateJobs(true);
        }
        $this->expectException(ClientException::class);
        $this->expectExceptionCode(403);
        $this->expectExceptionMessage('You don\'t have access to the resource.');
        $tokensApi->createToken($options);
    }

    public function testCanCreateJobsCannotModifyBranch(): void
    {
        $branchName = $this->generateDescriptionForTestObject();
        $devBranch = new DevBranches($this->getDeveloperStorageApiClient());
        $this->deleteBranchesByPrefix($devBranch, $branchName);
        $branch = $devBranch->createBranch($branchName);

        $pmBranchedClient = $this->getBranchAwareClient($branch['id'], $this->getClientOptionsForToken(STORAGE_API_TOKEN));
        $token = $pmBranchedClient->verifyToken();
        $this->assertTrue($token['canCreateJobs']);

        try {
            $pmWorkspaces = new Workspaces($pmBranchedClient);
            $pmWorkspaces->createWorkspace([], true);
            $this->fail('Production manager admin token with canCreateJobs should not be able to create bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $tokensApi = new Tokens($this->getDefaultClient());
        $newTokenWithCanCreateJobs = $tokensApi->createToken(
            (new TokenCreateOptions())
                ->setDescription($this->generateDescriptionForTestObject())
                ->setCanCreateJobs(true)
        );

        $tokenClient = $this->getBranchAwareClient($branch['id'], $this->getClientOptionsForToken($newTokenWithCanCreateJobs['token']));
        $token = $tokenClient->verifyToken();
        $this->assertTrue($token['canCreateJobs']);

        try {
            $pmWorkspaces = new Workspaces($tokenClient);
            $pmWorkspaces->createWorkspace([], true);
            $this->fail('Token with canCreateJobs should not be able to create bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }
}
