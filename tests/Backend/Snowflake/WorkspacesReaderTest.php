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

class WorkspacesReaderTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;


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
        $workspaces = new Workspaces($branchClient);

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

        $workspace['connection']['privateKey'] = $key->getPrivateKey();

        // create the connection after LOAD!! because the schema will be created by LOAD
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace, true);

        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data);

        $data = $db->fetchAll('languages_filtered');
        $this->assertCount(1, $data);
    }

    public function testLoadCloneToReaderAccount(): void
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
            ->setName('readerWS_clone')
            ->setDescription('some desc'));

        $components = new Components($branchClient);
        $workspaces = new Workspaces($branchClient);

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

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
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

        $workspace['connection']['privateKey'] = $key->getPrivateKey();

        // create the connection after LOAD!! because the schema will be created by LOAD
        $db = WorkspaceBackendFactory::createWorkspaceBackend($workspace, true);

        $data = $db->fetchAll('languages');
        $this->assertCount(5, $data);

        $data = $db->fetchAll('langs');
        $this->assertCount(5, $data);
    }

    public function testResetPublicKeyForReaderWorkspace(): void
    {
        $this->expectNotToPerformAssertions();

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
        $workspaces = new Workspaces($branchClient);

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

        //setup test tables
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
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
        $workspace['connection']['privateKey'] = $key->getPrivateKey();
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
}
