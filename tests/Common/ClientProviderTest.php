<?php
namespace Keboola\Test\ClientProvider;

use GuzzleHttp\Client as GuzzleClient;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\BranchAwareGuzzleClient;
use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

class ClientProviderTest extends StorageApiTestCase
{
    /**
     * @var ClientProvider
     */
    private $clientProvider;

    /**
     * @var Client
     */
    private $client;

    /**
     * @return void
     */
    public function setUp(): void
    {
        parent::setUp();

        $this->cleanupConfigurations($this->_client);

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createClientForCurrentTest();
    }

    /**
     * @dataProvider provideDefaultBranchClientProvider
     * @return void
     */
    public function testClientProviderForDefaultBranch(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);

        // check created client with DEFAULT branch
        $this->assertInstanceOf(Client::class, $this->client);

        // create branch aware client with DEFAULT branch - create branch has no effect
        $branchAwareClient = $this->clientProvider->createBranchAwareClientForCurrentTest([]);
        $this->assertInstanceOf(BranchAwareClient::class, $branchAwareClient);
        $this->assertSame($defaultBranchId, $branchAwareClient->getCurrentBranchId());

        // create guzzle client with DEFAULT branch - create branch has no effect
        $guzzleClient = $this->clientProvider->createGuzzleClientForCurrentTest($this->getGuzzleConfig());
        $this->assertInstanceOf(GuzzleClient::class, $guzzleClient);
    }

    /**
     * @dataProvider provideDevBranchClientProvider
     * @return void
     */
    public function testClientProviderForDevBranch(): void
    {
        $devBranchId = $this->clientProvider->getExistingBranchForTestCase()['id'];
        $defaultBranchId = $this->getDefaultBranchId($this);

        // check created client with DEV branch
        /** @var BranchAwareClient $client */
        $client = $this->client;
        $this->assertInstanceOf(BranchAwareClient::class, $client);
        $this->assertSame($devBranchId, $client->getCurrentBranchId());
        $this->assertNotSame($devBranchId, $defaultBranchId);

        // create branch aware client for DEV branch
        $branchAwareClient = $this->clientProvider->createBranchAwareClientForCurrentTest([], true);
        $this->assertInstanceOf(BranchAwareClient::class, $branchAwareClient);
        $this->assertSame($devBranchId, $branchAwareClient->getCurrentBranchId());

        // create branch aware client for DEV branch + create new branch
        $branchAwareClient2 = $this->clientProvider->createBranchAwareClientForCurrentTest([], false);
        $this->assertInstanceOf(BranchAwareClient::class, $branchAwareClient2);
        $devBranchId2 = $branchAwareClient2->getCurrentBranchId();
        $this->assertGreaterThan($devBranchId, $devBranchId2);

        // create guzzle client for DEV branch
        $guzzleClient = $this->clientProvider->createGuzzleClientForCurrentTest($this->getGuzzleConfig(), true);
        $this->assertInstanceOf(BranchAwareGuzzleClient::class, $guzzleClient);
        /** @var BranchAwareGuzzleClient $guzzleClient */
        $this->assertSame($devBranchId2, $guzzleClient->getCurrentBranchId());
    }

    /**
     * @return array
     */
    private function getGuzzleConfig()
    {
        return [
            'base_uri' => $this->client->getApiUrl(),
            'headers' => [
                'X-StorageApi-Token' => $this->client->getTokenString(),
            ],
        ];
    }

    /**
     * @return array
     */
    public function provideDefaultBranchClientProvider()
    {
        return [
            [ClientProvider::DEFAULT_BRANCH],
            ['invalidKey'],
        ];
    }

    /**
     * @return array
     */
    public function provideDevBranchClientProvider()
    {
        return [
            [ClientProvider::DEV_BRANCH],
        ];
    }

    /**
     * @dataProvider provideDevBranchName
     * @param string $clientType
     * @param int|string $dataName
     * @return void
     */
    public function testGetDevBranchName($clientType, $dataName): void
    {
        $branchNamePattern = '/^'
            . 'Keboola\\\\Test\\\\ClientProvider\\\\ClientProviderTest'
            . '\\\\'
            . 'testGetDevBranchName'
            . '\\\\'
            . '(0|1|dev|default)'
            . '\\\\'
            . '\d+'
            . '$/';

        $this->assertMatchesRegularExpression(
            $branchNamePattern,
            $this->clientProvider->getDevBranchName(),
        );
    }

    /**
     * @return array
     */
    public function provideDevBranchName()
    {
        return [
            0 => [ClientProvider::DEFAULT_BRANCH, 0],
            1 => [ClientProvider::DEV_BRANCH, 1],
            'default' => [ClientProvider::DEFAULT_BRANCH, 'default'],
            'dev' => [ClientProvider::DEV_BRANCH, 'dev'],
        ];
    }
}
