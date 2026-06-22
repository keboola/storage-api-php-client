<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Tokens;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use PHPUnit\Framework\TestCase;

class CreateTokenPrivilegedInProtectedDefaultBranchTest extends TestCase
{
    private function clientWithMock(MockHandler $mock, string $storageToken = 'my-storage-token'): Client
    {
        return new Client([
            'url' => 'https://connection.test',
            'token' => $storageToken,
            'handler' => $mock,
            'backoffMaxTries' => 0,
        ]);
    }

    public function testManageTokenModeSendsManageHeaderAlongsideStorageToken(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], (string) json_encode(['id' => '42']))]);
        $tokens = new Tokens($this->clientWithMock($mock, 'storage-tok'));

        $tokens->createTokenPrivilegedInProtectedDefaultBranch(
            (new TokenCreateOptions())->setDescription('test'),
            'manage-tok',
        );

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('manage-tok', $request->getHeaderLine('X-KBC-ManageApiToken'));
        self::assertSame('storage-tok', $request->getHeaderLine('X-StorageApi-Token'));
        self::assertSame('', $request->getHeaderLine('X-Kubernetes-Authorization'));
    }

    public function testForcesCanManageProtectedDefaultBranchFlagInRequestBody(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], (string) json_encode(['id' => '42']))]);
        $tokens = new Tokens($this->clientWithMock($mock));

        $tokens->createTokenPrivilegedInProtectedDefaultBranch(
            (new TokenCreateOptions())->setDescription('test'),
            'manage-tok',
        );

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        $body = json_decode((string) $request->getBody(), true);
        self::assertIsArray($body);
        self::assertTrue($body['canManageProtectedDefaultBranch']);
    }

    public function testNullApplicationTokenWithoutServiceAccountTokenThrowsClientException(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], (string) json_encode(['id' => '42']))]);
        $tokens = new Tokens($this->clientWithMock($mock));

        try {
            $tokens->createTokenPrivilegedInProtectedDefaultBranch(
                (new TokenCreateOptions())->setDescription('test'),
                null,
            );
            self::fail('Expected ClientException for missing service account token');
        } catch (ClientException $e) {
            self::assertStringContainsString('Service account token file', $e->getMessage());
            self::assertSame('serviceAccountTokenNotReadable', $e->getStringCode());
        }

        self::assertSame(1, $mock->count(), 'No HTTP request should be sent when SA token is unavailable');
    }

    public function testClientWrapperReturnsCreatedTokenId(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], (string) json_encode(['id' => '99']))]);
        $client = $this->clientWithMock($mock);

        $id = $client->createTokenPrivilegedInProtectedDefaultBranch(
            (new TokenCreateOptions())->setDescription('test'),
            'manage-tok',
        );

        self::assertSame('99', $id);
    }

    public function testClientWrapperAcceptsNullApplicationToken(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], (string) json_encode(['id' => '42']))]);
        $client = $this->clientWithMock($mock);

        try {
            $client->createTokenPrivilegedInProtectedDefaultBranch(
                (new TokenCreateOptions())->setDescription('test'),
                null,
            );
            self::fail('Expected ClientException for missing service account token');
        } catch (ClientException $e) {
            self::assertSame('serviceAccountTokenNotReadable', $e->getStringCode());
        }
    }
}
