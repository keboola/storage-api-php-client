<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;

class DevBranchesTest extends StorageApiTestCase
{
    private static $cleanupAfterClassTokenId;

    private static $teardownClient;

    /**
     * @dataProvider provideValidClients
     */
    public function testCreateBranch(Client $providedClient)
    {
        $providedToken = $providedClient->verifyToken();
        $branches = new DevBranches($providedClient);

        // cleanup
        $branchesList = $branches->listBranches();
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $branchesCreatedByThisTestMethod = array_filter(
            $branchesList,
            function ($branch) use ($branchName) {
                return strpos($branch['name'], $branchName) === 0;
            }
        );
        foreach ($branchesCreatedByThisTestMethod as $branch) {
            $branches->deleteBranch($branch['id']);
        }

        // test

        // can create branch
        $branch = $branches->createBranch($branchName);
        $this->assertArrayHasKey('id', $branch);
        $this->assertArrayHasKey('name', $branch);
        $this->assertArrayHasKey('created', $branch);
        $this->assertArrayHasKey('isDefault', $branch);
        $this->assertArrayNotHasKey('admin', $branch);
        $this->assertArrayHasKey('creatorToken', $branch);
        $this->assertArrayHasKey('id', $branch['creatorToken']);
        $this->assertEquals($providedToken['id'], $branch['creatorToken']['id']);
        $this->assertArrayHasKey('name', $branch['creatorToken']);
        $this->assertSame($providedToken['description'], $branch['creatorToken']['name']);
        $this->assertSame($branchName, $branch['name']);
        $branchId = $branch['id'];

        // event is created for created branch
        $event = $this->findLastEvent($providedClient, [
            'event' => 'storage.devBranchCreated',
            'objectId' => $branchId,
        ]);
        $this->assertSame($branchName, $event['objectName']);
        $this->assertSame('devBranch', $event['objectType']);

        // can get branch detail
        $branchFromDetail = $branches->getBranch($branchId);
        $this->assertArrayHasKey('id', $branchFromDetail);
        $this->assertArrayHasKey('name', $branchFromDetail);
        $this->assertArrayHasKey('created', $branchFromDetail);
        $this->assertArrayHasKey('isDefault', $branch);
        $this->assertArrayNotHasKey('admin', $branchFromDetail);
        $this->assertArrayHasKey('creatorToken', $branchFromDetail);
        $this->assertArrayHasKey('id', $branchFromDetail['creatorToken']);
        $this->assertEquals($providedToken['id'], $branchFromDetail['creatorToken']['id']);
        $this->assertArrayHasKey('name', $branchFromDetail['creatorToken']);
        $this->assertSame($providedToken['description'], $branchFromDetail['creatorToken']['name']);

        $buckets = $this->_client->listBuckets([], $branchId);
        $this->assertCount(0, $buckets);

        // can list branches and see created branch
        $branchList = $branches->listBranches();
        $this->assertGreaterThanOrEqual(1, count($branchList));
        $this->assertContains($branchFromDetail, $branchList);

        $defaultBranchCount = 0;
        foreach ($branchesList as $branch) {
            if ($branch['isDefault'] === true) {
                $defaultBranchCount ++;
            }
        }

        $this->assertSame(1, $defaultBranchCount);

        // cannot create branch with same name
        try {
            $branches->createBranch($branchName);
            $this->fail('Sharing bucket to non organization member should fail.');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('There already is a branch with name "%s"', $branchName),
                $e->getMessage()
            );
            $this->assertSame('devBranch.duplicateName', $e->getStringCode());
        }

        // can delete branch
        $branches->deleteBranch($branchId);

        // there is event for deleted branch
        $this->findLastEvent($providedClient, [
            'event' => 'storage.devBranchDeleted',
            'objectId' => $branchId,
        ]);

        // cannot delete nonexistent branch
        try {
            $branches->deleteBranch($branchId);
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Branch not found'),
                $e->getMessage()
            );
        }

        // now branch can be created with same name as deleted branch
        $newBranch = $branches->createBranch($branchName);
    }

    public function testOrgAdminCanDeleteBranchCreatedByAdmin()
    {
        $guestClient = $this->getGuestStorageApiClient();

        $branches = new DevBranches($guestClient);
        $branchName = __CLASS__ . ' příliš žluťoučký kůň' . microtime();

        $branch = $branches->createBranch($branchName);

        $branchId = $branch['id'];

        $orgAdminClient = $this->getDefaultClient();
        $branches = new DevBranches($orgAdminClient);

        $branches->deleteBranch($branchId);

        $this->expectNotToPerformAssertions();
    }

    public function testCannotDeleteNotOwnedBranch()
    {
        $orgAdminClient = $this->getDefaultClient();
        $branches = new DevBranches($orgAdminClient);

        $branchName = __CLASS__ . ' příliš žluťoučký kůň' . microtime();

        $branch = $branches->createBranch($branchName);

        $branchId = $branch['id'];

        $guestClient = $this->getGuestStorageApiClient();
        $branches = new DevBranches($guestClient);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Only owner or organization admin can manipulate a development branch');

        $branches->deleteBranch($branchId);
    }

    public function provideValidClients()
    {
        $guest = $this->getGuestStorageApiClient();
        return [
            'admin' => [$this->getDefaultClient()],
            'guest' => [$guest],
        ];
    }

    /**
     * @dataProvider provideInvalidClients
     */
    public function testInsufficientClientCannotCreateBranch(Client $providedClient)
    {
        $branches = new DevBranches($providedClient);

        $branchName = __CLASS__ . time();

        $this->expectExceptionMessage('You don\'t have access to resource.');
        $this->expectException(ClientException::class);

        $branches->createBranch($branchName);
    }

    public function provideInvalidClients()
    {
        self::$teardownClient = $this->getDefaultClient();
        $tokenId = self::$teardownClient->createToken(new TokenCreateOptions());
        $token = self::$teardownClient->getToken($tokenId);
        self::$cleanupAfterClassTokenId = $token['id'];

        $notAdminClient = $this->getClientForToken($token['token']);

        return [
            'not admin' => [$notAdminClient],
        ];
    }

    public static function tearDownAfterClass()
    {
        parent::tearDownAfterClass();
        if (self::$cleanupAfterClassTokenId) {
            self::$teardownClient->dropToken(self::$cleanupAfterClassTokenId);
        }
        self::$teardownClient = null;
    }
}
