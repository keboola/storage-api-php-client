<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\Helpers\ClientsProvider;
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
        $providedClient->verifyToken();
        $branches = new DevBranches($providedClient);

        $branchName = __CLASS__ . ' příliš žluťoučký kůň' . microtime();
        $branch = $branches->createBranch($branchName);

        $this->assertArrayHasKey('created', $branch);
        unset($branch['created']);
        $this->assertArrayHasKey('id', $branch);
        $branchId = $branch['id'];
        unset($branch['id']);
        $this->assertSame($branchName, $branch['name']);

        // test branch create event
        $event = $this->findLastEvent($providedClient, [
            'event' => 'storage.devBranchCreated',
            'objectId' => $branchId
        ]);
        $this->assertSame($branchName, $event['objectName']);
        $this->assertSame('devBranch', $event['objectType']);

        try {
            $branches->createBranch($branchName);
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('There already is a branch with name "%s"', $branchName),
                $e->getMessage()
            );
            $this->assertSame('devBranch.duplicateName', $e->getStringCode());
        }

        $branches->deleteBranch($branchId);

        $this->findLastEvent($providedClient, [
            'event' => 'storage.devBranchDeleted',
            'objectId' => $branchId
        ]);

        try {
            $branches->deleteBranch($branchId);
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Branch not found'),
                $e->getMessage()
            );
        }

        // now branch can be created
        $newBranch = $branches->createBranch($branchName);

        $branches->deleteBranch($newBranch['id']);
    }

    public function testOrgAdminCanDeleteBranchCreatedByAdmin()
    {
        $guestClient = ClientsProvider::getGuestStorageApiClient();

        $branches = new DevBranches($guestClient);
        $branchName = __CLASS__ . ' příliš žluťoučký kůň' . microtime();

        $branch = $branches->createBranch($branchName);

        $branchId = $branch['id'];

        $orgAdminClient = ClientsProvider::getClient();
        $branches = new DevBranches($orgAdminClient);

        $branches->deleteBranch($branchId);

        $this->expectNotToPerformAssertions();
    }

    public function testCannotDeleteNotOwnedBranch()
    {
        $orgAdminClient = ClientsProvider::getClient();
        $branches = new DevBranches($orgAdminClient);

        $branchName = __CLASS__ . ' příliš žluťoučký kůň' . microtime();

        $branch = $branches->createBranch($branchName);

        $branchId = $branch['id'];

        $guestClient = ClientsProvider::getGuestStorageApiClient();
        $branches = new DevBranches($guestClient);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Only owner or organization admin can delete development branch');

        $branches->deleteBranch($branchId);
    }

    public function provideValidClients()
    {
        $guest = ClientsProvider::getGuestStorageApiClient();
        return [
            'admin' => [ClientsProvider::getClient()],
            'guest' => [$guest]
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
        self::$teardownClient = ClientsProvider::getClient();
        $tokenId = self::$teardownClient->createToken(new TokenCreateOptions());
        $token = self::$teardownClient->getToken($tokenId);
        self::$cleanupAfterClassTokenId = $token['id'];

        $notAdminClient = ClientsProvider::getClientForToken($token['token']);

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
