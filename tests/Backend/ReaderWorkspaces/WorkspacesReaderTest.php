<?php

namespace Backend\ReaderWorkspaces;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException;
use Exception;
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
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Utils\PemKeyCertificateGenerator;
use Keboola\Test\Utils\SnowflakeConnectionUtils;

class WorkspacesReaderTest extends WorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;
    use SnowflakeConnectionUtils;

    private static ?Client $client = null;

    private static ?Connection $backendConnection = null;

    public function setUp(): void
    {
        parent::setUp();

        $components = new Components($this->_client);
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
        $filteredReaderWorkspaces = array_filter($workspaces->listWorkspaces(), static fn($workspace) => $workspace['platformUsageType'] === 'reader');
        foreach ($filteredReaderWorkspaces as $workspace) {
            $workspaces->deleteWorkspace($workspace['id']);
        }

        // drop all readers accounts
        self::dropReaderAccounts();

        // ensure the workspace and the reader account is cleaned up.
        self::ensureReaderAccountIsRemoved();
    }

    public function testLoadToReaderAccount(): void
    {
        $workspaces = $this->createWorkspaces();
        $workspace = $this->prepareWorkspace();

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languages_filtered',
                    'overwrite' => false,
                    'whereColumn' => 'id',
                    'whereValues' => [1],
                    'whereOperator' => 'eq',
                ],
            ],
        ]);

        // create the connection after LOAD!! because the schema will be created by LOAD
        $db = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);

        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data);

        $data = $db->fetchAll('languages_filtered');
        $this->assertCount(1, $data);
    }

    public function testDenyUnloadOnReaderWorkspace(): void
    {
        $workspaces = $this->createWorkspaces();
        $workspace = $this->prepareWorkspace();

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languages_filtered',
                    'overwrite' => false,
                    'whereColumn' => 'id',
                    'whereValues' => [1],
                    'whereOperator' => 'eq',
                ],
            ],
        ]);

        try {
            $this->_client->writeTableAsyncDirect(
                $tableId,
                [
                    'dataWorkspaceId' => $workspace['id'],
                    'dataObject' => 'languagesLoaded/',
                    'columns' => ['id', 'name'],
                ],
            );
            self::fail('Writing to a table in a reader workspace should fail.');
        } catch (Exception $exception) {
            self::assertStringContainsString('Table import for reader workspaces is not supported.', $exception->getMessage());
        }
    }

    public function testDenyExecuteQueryOnReaderWorkspace(): void
    {
        $workspaces = $this->createWorkspaces();
        $workspace = $this->prepareWorkspace();

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languages_filtered',
                    'overwrite' => false,
                    'whereColumn' => 'id',
                    'whereValues' => [1],
                    'whereOperator' => 'eq',
                ],
            ],
        ]);

        try {
            $workspaces->executeQuery(
                $workspace['id'],
                'SELECT * FROM languages',
            );
            self::fail('Executing query on reader workspace should fail.');
        } catch (Exception $exception) {
            self::assertStringContainsString('Custom query execution is not supported for reader workspace.', $exception->getMessage());
        }
    }

    public function testDenyDeleteRowsOnReaderWorkspace(): void
    {
        $workspaces = $this->createWorkspaces();
        $workspace = $this->prepareWorkspace();

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languages_filtered',
                    'overwrite' => false,
                    'whereColumn' => 'id',
                    'whereValues' => [1],
                    'whereOperator' => 'eq',
                ],
            ],
        ]);

        try {
            $this->_client->deleteTableRows($tableId, [
                'whereFilters' => [
                    [
                        'column' => 'id',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => $workspace['id'],
                            'table' => 'languages',
                            'column' => 'id',
                        ],
                    ],
                ],
            ]);
            self::fail('Deleting rows from a table in a reader workspace should fail.');
        } catch (Exception $exception) {
            self::assertStringContainsString('Cannot delete rows from a table in a reader workspace.', $exception->getMessage());
        }
    }

    public function testLoadCloneToReaderAccount(): void
    {
        $workspaces = $this->createWorkspaces();
        $workspace = $this->prepareWorkspace();

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'langs',
                ],
            ],
        ]);

        // create the connection after LOAD!! because the schema will be created by LOAD
        $db = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);

        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data);

        $data = $db->fetchAll('langs');
        $this->assertCount(5, $data);
    }

    public function testResetPublicKeyForReaderWorkspace(): void
    {
        $workspaces = $this->createWorkspaces();
        $workspace = $this->prepareWorkspace();

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                ],
                [
                    'source' => $tableId,
                    'destination' => 'langs',
                ],
            ],
        ]);

        // create the connection after LOAD!! because the schema will be created by LOAD
        $connection = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $connection->executeQuery('SELECT 1;');

        $newKey = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $workspaces->setPublicKey($workspace['id'], new Workspaces\SetPublicKeyRequest(publicKey: $newKey->getPublicKey()));

        // new key should work
        $workspace['connection']['privateKey'] = $newKey->getPrivateKey();
        $newConnection = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $newConnection->executeQuery('SELECT 1;');

        // Cannot work with old key
        try {
            $connection->getDb()->close();
            $connection->getDb()->executeQuery('SELECT 1;');
            self::fail('Old key should not work');
        } catch (DriverException $driverException) {
            self::assertStringContainsString('JWT token is invalid', $driverException->getMessage());
        }

        // New should be working always
        $newConnection->getDb()->close();
        $newConnection->getDb()->executeQuery('SELECT 1;');
    }

    private function prepareWorkspace(?array $options = null): array
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        // create configuration
        $components = new Components($branchClient);
        $components->addConfiguration((new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('readerWS')
            ->setDescription('some desc'));

        $components = new Components($branchClient);

        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);

        $workspace = $components->createConfigurationWorkspace(
            $componentId,
            $configurationId,
            $options ?? [
                'useCase' => 'reader',
                'backend' => 'snowflake',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
                'publicKey' => $key->getPublicKey(),
            ],
        );

        $workspace['connection']['privateKey'] = $key->getPrivateKey();

        return $workspace;
    }

    private function createWorkspaces(): Workspaces
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);

        return new Workspaces($branchClient);
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
