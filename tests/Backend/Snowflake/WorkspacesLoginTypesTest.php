<?php

declare(strict_types=1);

namespace Backend\Snowflake;

use Generator;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\PemKeyCertificateGenerator;

class WorkspacesLoginTypesTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    /**
     * @return Generator<string, array{async:bool}>
     */
    public static function syncAsyncProvider(): Generator
    {
        yield 'sync' => [
            'async' => false,
        ];
        yield 'async' => [
            'async' => true,
        ];
    }

    public static function createWorkspaceProvider(): Generator
    {
        foreach (self::syncAsyncProvider() as $name => $syncAsync) {
            yield 'default ' . $name => [
                'loginType' => null,
                'async' => $syncAsync['async'],
                'expectedLoginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD->value,
            ];
            yield 'legacy login type ' . $name => [
                'loginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD,
                'async' => $syncAsync['async'],
                'expectedLoginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD->value,
            ];
        }
    }

    /**
     * @dataProvider createWorkspaceProvider
     */
    public function testWorkspaceCreate(
        WorkspaceLoginType|null $loginType,
        bool $async,
        string $expectedLoginType
    ): void {
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $options = [];
        if ($loginType !== null) {
            $options['loginType'] = $loginType;
        }
        $workspace = $this->initTestWorkspace('snowflake', $options, true, $async);

        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertSame('snowflake', $connection['backend']);
        $this->assertSame($expectedLoginType, $connection['loginType']);

        // test connection is working
        $this->getDbConnectionSnowflake($connection);

        $workspaces->deleteWorkspace($workspace['id'], [], true);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceCreatedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceCreatedEvent['runId']);
            $this->assertSame('storage.workspaceCreated', $workspaceCreatedEvent['event']);
            $this->assertSame('storage', $workspaceCreatedEvent['component']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceCreated')->setRunId($runId);

        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);

        $assertCallback = function ($events) use ($runId) {
            $this->assertCount(1, $events);
            $workspaceDeletedEvent = array_pop($events);
            $this->assertSame($runId, $workspaceDeletedEvent['runId']);
            $this->assertSame('storage.workspaceDeleted', $workspaceDeletedEvent['event']);
            $this->assertSame('storage', $workspaceDeletedEvent['component']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceDeleted')->setRunId($runId);
        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);
        $this->assertCredentialsShouldNotWork($connection);
    }

    public function testWorkspaceWithSsoLoginType(): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $options = [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
        ];
        $workspace = $this->initTestWorkspace('snowflake', $options, true, true);

        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertSame('snowflake', $connection['backend']);
        $this->assertSame('snowflake-person-sso', $connection['loginType']);
        $this->assertArrayNotHasKey('password', $connection);

        // we are not testing working connection as there is no way to connect than SSO

        try {
            // try reset password
            $workspaces->resetWorkspacePassword($workspace['id']);
            $this->fail('Password reset should not be supported for SSO login type');
        } catch (ClientException $e) {
            $this->assertSame($e->getCode(), 400);
            $this->assertSame('workspace.resetPasswordNotSupported', $e->getStringCode());
        }
    }

    public function keyPairLoginTypeProvider(): Generator
    {
        yield 'service keypair' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            'expectedLoginType' => 'snowflake-service-keypair',
        ];
        yield 'person keypair' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
            'expectedLoginType' => 'snowflake-person-keypair',
        ];
    }

    /**
     * @dataProvider keyPairLoginTypeProvider
     */
    public function testPersonWorkspaceWithKeyPair(WorkspaceLoginType $loginType, string $expectedLoginType): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $options = [
            'loginType' => $loginType,
            'publicKey' => $key->getPublicKey(),
        ];
        $workspace = $this->initTestWorkspace(
            'snowflake',
            $options,
            true,
        );

        /** @var array $connection */
        $connection = $workspace['connection'];
        $this->assertSame('snowflake', $connection['backend']);
        $this->assertSame($expectedLoginType, $connection['loginType']);
        $this->assertArrayNotHasKey('password', $connection);

        $workspace['connection']['privateKey'] = $key->getPrivateKey();

        $backend = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);

        $backend->getDb()->executeQuery('SELECT 1');

        try {
            // try reset password
            $workspaces->resetWorkspacePassword($workspace['id']);
            $this->fail('Password reset should not be supported for keypair login type');
        } catch (ClientException $e) {
            $this->assertSame($e->getCode(), 400);
            $this->assertSame('workspace.resetPasswordNotSupported', $e->getStringCode());
        }
    }

    /**
     * @dataProvider keyPairLoginTypeProvider
     */
    public function testWorkspaceKeyPairRotation(WorkspaceLoginType $loginType, string $expectedLoginType): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $options = [
            'loginType' => $loginType,
            'publicKey' => $key->getPublicKey(),
        ];
        $workspace = $this->initTestWorkspace(
            'snowflake',
            $options,
            true,
        );

        $workspace['connection']['privateKey'] = $key->getPrivateKey();
        $backendKey1 = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $backendKey1->getDb()->executeQuery('SELECT 1');

        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $workspaces->setPublicKey($workspace['id'], new Workspaces\SetPublicKeyRequest(publicKey: $key->getPublicKey()));

        // old works KEY_1
        $backendKey1->getDb()->executeQuery('SELECT 1');

        // new works KEY_2
        $workspace['connection']['privateKey'] = $key->getPrivateKey();
        $backendKey2 = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $backendKey2->getDb()->executeQuery('SELECT 1');

        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $workspaces->setPublicKey($workspace['id'], new Workspaces\SetPublicKeyRequest(publicKey: $key->getPublicKey()));

        // old KEY_1 is rotated
        try {
            $backendKey1->getDb()->executeQuery('SELECT 1');
            $this->fail('Old key should not work');
        } catch (\Throwable) {
            // OK
        }

        // new works KEY_1
        $workspace['connection']['privateKey'] = $key->getPrivateKey();
        $backendKey1_2 = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $backendKey1_2->getDb()->executeQuery('SELECT 1');
        // old KEY_2 works
        $backendKey2->getDb()->executeQuery('SELECT 1');

        // rotate again KEY_1
        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $workspaces->setPublicKey($workspace['id'], new Workspaces\SetPublicKeyRequest(
            publicKey: $key->getPublicKey(),
            keyName: Workspaces\PublicKeyName::RSA_PUBLIC_KEY_1,
        ));
        // old KEY_2 works
        $backendKey2->getDb()->executeQuery('SELECT 1');

        // old KEY_1_2 is rotated
        try {
            $backendKey1_2->getDb()->executeQuery('SELECT 1');
            $this->fail('Old key should not work');
        } catch (\Throwable) {
            // OK
        }

        // new KEY_1_3 works
        $workspace['connection']['privateKey'] = $key->getPrivateKey();
        $backendKey1_3 = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $backendKey1_3->getDb()->executeQuery('SELECT 1');
    }
}
