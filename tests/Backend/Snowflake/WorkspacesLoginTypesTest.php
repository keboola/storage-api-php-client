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
        string $expectedLoginType,
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
    public function testWorkspaceWithKeyPair(WorkspaceLoginType $loginType, string $expectedLoginType): void
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

        // test universal resetCredentials method
        try {
            $workspaces->resetCredentials($workspace['id'], new Workspaces\ResetCredentialsRequest());
            $this->fail('Password reset should not be supported for keypair login type');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), sprintf(
                'Workspace with login type "%s" requires "publicKey" credentials.',
                $loginType->value,
            ));
        }

        // resetCredentials works with publicKey
        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $workspaces->resetCredentials($workspace['id'], new Workspaces\ResetCredentialsRequest(
            publicKey: $key->getPublicKey(),
        ));
        $workspace['connection']['privateKey'] = $key->getPrivateKey();
        $backendKey1 = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $backendKey1->getDb()->executeQuery('SELECT 1');
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

        // try set invalid public key
        try {
            $workspaces->setPublicKey($workspace['id'], new Workspaces\SetPublicKeyRequest(
                publicKey: 'this-is-not-a-valid-rsa-public-key',
            ));
            $this->fail('Invalid public key should not be accepted');
        } catch (ClientException $e) {
            $this->assertSame($e->getCode(), 400);
            $this->assertSame('validation.failed', $e->getStringCode());
        }
    }

    public static function provideLoginTypesWithPasswordCredentials(): iterable
    {
        yield 'no loginType' => [
            'loginType' => null,
        ];

        yield 'password loginType' => [
            'loginType' => WorkspaceLoginType::DEFAULT,
        ];
    }

    /** @dataProvider provideLoginTypesWithPasswordCredentials */
    public function testResetPasswordCredentials(?WorkspaceLoginType $loginType): void
    {
        $workspaces = $this->createPartialMock(Workspaces::class, [
            'getWorkspace',
            'resetWorkspacePassword',
            'setPublicKey',
        ]);
        $workspaces->expects(self::once())
            ->method('getWorkspace')
            ->with('123')
            ->willReturn([
                'connection' => [
                    'loginType' => $loginType?->value,
                ],
            ]);
        $workspaces->expects(self::once())
            ->method('resetWorkspacePassword')
            ->with('123')
            ->willReturn([
                'password' => 'new-password',
            ]);
        $workspaces->expects(self::never())->method('setPublicKey');

        $result = $workspaces->resetCredentials('123', new Workspaces\ResetCredentialsRequest());
        self::assertSame([
            'password' => 'new-password',
        ], $result);
    }

    public function testResetKeyPairCredentials(): void
    {
        $workspaces = $this->createPartialMock(Workspaces::class, [
            'getWorkspace',
            'resetWorkspacePassword',
            'setPublicKey',
        ]);
        $workspaces->expects(self::once())
            ->method('getWorkspace')
            ->with('123')
            ->willReturn([
                'connection' => [
                    'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR->value,
                ],
            ]);
        $workspaces->expects(self::never())->method('resetWorkspacePassword');
        $workspaces->expects(self::once())
            ->method('setPublicKey')
            ->with('123', new Workspaces\SetPublicKeyRequest(publicKey: 'publicKey'));

        $result = $workspaces->resetCredentials('123', new Workspaces\ResetCredentialsRequest(publicKey: 'publicKey'));
        self::assertSame([], $result);
    }

    public static function provideInvalidResetCredentialsRequestParameters(): iterable
    {
        yield 'password loginType with publicKey' => [
            'loginType' => WorkspaceLoginType::DEFAULT,
            'request' => new Workspaces\ResetCredentialsRequest(
                publicKey: 'publicKey',
            ),
            'expectedError' => 'Workspace with login type "default" does not support "publicKey" credentials.',
        ];

        yield 'keypair loginType without publicKey' => [
            'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            'request' => new Workspaces\ResetCredentialsRequest(),
            'expectedError' => 'Workspace with login type "snowflake-service-keypair" requires "publicKey" credentials.',
        ];
    }

    /** @dataProvider provideInvalidResetCredentialsRequestParameters */
    public function testResetCredentialsWithInvalidParametersThrowsException(
        WorkspaceLoginType $loginType,
        Workspaces\ResetCredentialsRequest $request,
        string $expectedError,
    ): void {
        $workspaces = $this->createPartialMock(Workspaces::class, [
            'getWorkspace',
            'resetWorkspacePassword',
            'setPublicKey',
        ]);
        $workspaces->expects(self::once())
            ->method('getWorkspace')
            ->with('123')
            ->willReturn([
                'connection' => [
                    'loginType' => $loginType->value,
                ],
            ]);
        $workspaces->expects(self::never())->method('resetWorkspacePassword');
        $workspaces->expects(self::never())->method('setPublicKey');

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedError);

        $workspaces->resetCredentials('123', $request);
    }
}
