<?php

namespace Backend\Snowflake;

use Doctrine\DBAL\Exception\DriverException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Utils\PemKeyCertificateGenerator;
use Keboola\Test\Utils\SnowflakeConnectionUtils;

class WorkspacesReaderTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;
    use SnowflakeConnectionUtils;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();

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
        $this->expectNotToPerformAssertions();

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
            $this->fail('Old key should not work');
        } catch (DriverException $driverException) {
            // Should throw exception
        }

        // New should be working always
        $newConnection->getDb()->close();
        $newConnection->getDb()->executeQuery('SELECT 1;');
    }

    public function testRemoveWorkspace(): void
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

        $backendConnection = $this->ensureSnowflakeConnection();
        $verifiedToken = $this->_client->verifyToken();
        $organizationId = $verifiedToken['organization']['id'];
        $result = $backendConnection->fetchAllAssociative("SHOW MANAGED ACCOUNTS LIKE '%_READER_ACCOUNT_{$organizationId}';");

        self::assertTrue(count($result) === 1);

        $connection = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);

        $workspaces->deleteWorkspace($workspace['id']);

        // Cannot execute query on removed workspace
        try {
            $connection->executeQuery('SELECT 1;');
            $this->fail('Removed workspace should not be accessible');
        } catch (DriverException $driverException) {
            // Should throw exception
        }

        $result = $backendConnection->fetchAllAssociative("SHOW MANAGED ACCOUNTS LIKE '%_READER_ACCOUNT_{$organizationId}';");

        self::assertTrue(count($result) === 0);
    }

    private function prepareWorkspace(): array
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

    private function createWorkspaces(): Workspaces
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);

        return new Workspaces($branchClient);
    }
}
