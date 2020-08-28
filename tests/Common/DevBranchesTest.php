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
        $token = $providedClient->verifyToken();
        $branches = new DevBranches($providedClient);

        $branchName = __CLASS__ . microtime();
        $branch = $branches->createBranch($branchName);
        $adminId = $token['admin']['id'];
        $projectId = $token['owner']['id'];

        $this->assertArrayHasKey('created', $branch);
        unset($branch['created']);
        $this->assertArrayHasKey('id', $branch);
        $branchId = $branch['id'];
        unset($branch['id']);
        $this->assertSame($branchName, $branch['name']);
        $this->assertArrayHasKey('owner', $branch);
        $this->assertSame(['id', 'name'], array_keys($branch['owner']));
        $this->assertSame($projectId, $branch['owner']['id']);
        $this->assertArrayHasKey('admin', $branch);
        $this->assertSame(['id', 'name', 'email'], array_keys($branch['admin']));
        $this->assertSame($adminId, $branch['admin']['id']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('There already is a branch with name "%s"', $branchName));
        $event = $this->findLastEvent($providedClient, [
            'event' => 'storage.devBranchCreated',
            'objectId' => $branchId
        ]);
        $this->assertSame($branchName, $event['objectName']);

        $branches->createBranch($branchName);
    }

    private function findLastEvent(Client $client, array $filter)
    {
        $this->createAndWaitForEvent(
            (new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'),
            $client
        );
        $events = $client->listTokenEvents($client->verifyToken()['id']);
        foreach ($events as $event) {
            foreach ($filter as $key => $value) {
                if ($event[$key] != $value) {
                    continue 2;
                }
            }
            return $event;
        }
        $this->fail(sprintf('Event for filter "%s" does not exist', (string) json_encode($filter)));
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
