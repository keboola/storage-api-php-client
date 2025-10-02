<?php

namespace Backend\ReaderWorkspaces;

use Doctrine\DBAL\Connection;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\PemKeyCertificateGenerator;
use Keboola\Test\Utils\SnowflakeConnectionUtils;

class WorkspacesReaderSharingTest extends StorageApiTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;
    use SnowflakeConnectionUtils;

    private static ?Client $client = null;

    private static ?Connection $backendConnection = null;

    public function setUp(): void
    {
        parent::setUp();
        $linkClient = $this->getClient([
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $components = new Components($linkClient);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        if (self::$client === null) {
            self::$client = $this->_client;
        }

        if (self::$backendConnection === null) {
            self::$backendConnection = $this->ensureSnowflakeConnection();
        }

        // drop all reader workspaces before test
        $workspaces = new Workspaces(self::$client);
        $filteredReaderWorkspaces = array_filter($workspaces->listWorkspaces(), static fn($workspace
        ) => $workspace['platformUsageType'] === 'reader');
        foreach ($filteredReaderWorkspaces as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }

        // drop all readers accounts
        self::dropReaderAccounts();

        // ensure the workspace and the reader account is cleaned up.
        self::ensureReaderAccountIsRemoved();

        // unlink buckets
        $linkedBuckets = $linkClient->listBuckets();
        foreach ($linkedBuckets as $bucket) {
            $linkClient->dropBucket($bucket['id'], ['force' => true]);
        }

        $shareClient = $this->getClient([
            'token' => STORAGE_API_SHARE_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        foreach ($shareClient->listBuckets() as $bucket) {
            if ($shareClient->isSharedBucket($bucket['id'])) {
                $shareClient->unshareBucket($bucket['id']);
            }
        }

        $this->_initEmptyTestBuckets();
    }

    public function testShareLinkAndLoadTableToReaderWorkspace(): void
    {
        $linkClient = $this->getClient([
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        // Step 1: Create a source bucket and table
        $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $shareClient = $this->getClient([
            'token' => STORAGE_API_SHARE_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $tokenInfo = $linkClient->verifyToken();
        $projectId = $tokenInfo['owner']['id'];
        // Step 2: Share the bucket with the organization
        $shareClient->shareBucketToProjects($this->getTestBucketId(), [$projectId]);

        // Step 3: Link the shared bucket to the current project
        $response = $linkClient->listSharedBuckets();
        $this->assertCount(1, $response);
        $sharedBucket = reset($response);
        $hashedUniqueTableName = sha1('linked-' . $this->generateDescriptionForTestObject());
        $linkedBucketId = $linkClient->linkBucket(
            $hashedUniqueTableName,
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
            null,
            false,
        );

        // Step 3.1: Create another table in the linked project that is not linked
        $nativeBucketInLinkedProject = $linkClient->createBucket($this->getTestBucketName($this->generateDescriptionForTestObject()), self::STAGE_IN);
        $notLinkedTable = $linkClient->createTableAsync(
            $nativeBucketInLinkedProject,
            'not-linked-table',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        // Step 4: Create a reader workspace
        $workspaces = new Workspaces($linkClient);
        $workspace = $this->prepareWorkspace($linkClient);

        // Step 5: Load the linked table into the reader workspace
        $linkedTableId = $linkedBucketId . '.languages';
        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $notLinkedTable,
                    'destination' => 'not-linked-table',
                ],
                [
                    'source' => $linkedTableId,
                    'destination' => 'languages',
                ],
            ],
        ]);

        // Step 6: Assert that the data is loaded correctly
        $db = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data); // languages.csv has 5 rows
        $data = $db->fetchAll('not-linked-table');
        $this->assertCount(5, $data); // languages.csv has 5 rows
    }

    private function prepareWorkspace(Client $client): array
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';

        // create configuration
        $components = new Components($client);
        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('readerWS')
            ->setDescription('some desc'));

        $components = new Components($client);

        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);

        $workspace = $components->createConfigurationWorkspace(
            $componentId,
            $configurationId,
            [
                'useCase' => 'reader',
                'backend' => 'snowflake',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
                'publicKey' => $key->getPublicKey(),
            ],
        );

        $workspace['connection']['privateKey'] = $key->getPrivateKey();

        return $workspace;
    }

    private static function dropReaderAccounts(): void
    {
        $accounts = self::fetchReadersAccountsForCurrentOrganization();

        foreach ($accounts as $account) {
            self::$backendConnection?->executeQuery(
                sprintf('DROP MANAGED ACCOUNT %s;', $account['account_name']),
            );
        }
    }

    private static function ensureReaderAccountIsRemoved(): void
    {
        $accounts = self::fetchReadersAccountsForCurrentOrganization();

        self::assertCount(0, $accounts);
    }

    /**
     * @return array<array{account_name: string}>
     * @throws \Doctrine\DBAL\Exception
     */
    private static function fetchReadersAccountsForCurrentOrganization(): array
    {
        $verifiedToken = self::$client?->verifyToken();
        $organizationId = $verifiedToken['organization']['id'] ?? '';

        /** @var array<array{account_name: string}> $result */
        $result = self::$backendConnection?->fetchAllAssociative(
            sprintf(
                'SHOW MANAGED ACCOUNTS LIKE %s;',
                SnowflakeQuote::quote(sprintf('%%_READER_ACCOUNT_%s', $organizationId)),
            ),
        ) ?? [];

        return $result;
    }
}
