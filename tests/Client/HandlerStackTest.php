<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Client;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\HandlerStack;
use PHPUnit\Framework\TestCase;

class HandlerStackTest extends TestCase
{
    public function testCreateReturnsHandlerStack(): void
    {
        $handlerStack = HandlerStack::create();

        self::assertNotNull($handlerStack);
    }

    public function testCreateWithCustomHandler(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create(['handler' => $mockHandler]);
        $client = new GuzzleClient(['handler' => $handlerStack]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(200, $response->getStatusCode());
    }

    public function testNoRetryWhen501NotImplemented(): void
    {
        $mockHandler = new MockHandler([
            new Response(501, [], 'Not Implemented'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(501, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testNoRetryWhen503AndRetryOnMaintenanceDisabled(): void
    {
        $mockHandler = new MockHandler([
            new Response(503, [], 'Service Unavailable'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
            'retryOnMaintenance' => false,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(503, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testRetryWhen503AndRetryOnMaintenanceEnabled(): void
    {
        $mockHandler = new MockHandler([
            new Response(503, [], 'Service Unavailable'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
            'retryOnMaintenance' => true,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(200, $response->getStatusCode());
        self::assertSame(0, $mockHandler->count(), 'Both responses should be consumed');
    }

    public function testRetryOn5xxErrors(): void
    {
        $mockHandler = new MockHandler([
            new Response(500, [], 'Internal Server Error'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(200, $response->getStatusCode());
        self::assertSame(0, $mockHandler->count(), 'Both responses should be consumed');
    }

    public function testNoRetryWhenMaxRetriesExceeded(): void
    {
        $mockHandler = new MockHandler([
            new Response(500, [], 'Internal Server Error'),
            new Response(500, [], 'Internal Server Error'),
            new Response(500, [], 'Internal Server Error'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 2,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(500, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Should stop after max retries');
    }

    public function testRetryOn409VersionConflict(): void
    {
        $versionConflictBody = (string) json_encode([
            'code' => 'storage.components.configurations.versionConflict',
            'message' => 'Configuration row creation conflict. Please retry the operation.',
        ]);

        $mockHandler = new MockHandler([
            new Response(409, [], $versionConflictBody),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(200, $response->getStatusCode());
        self::assertSame(0, $mockHandler->count(), 'Both responses should be consumed');
    }

    public function testNoRetryOn409WithDifferentErrorCode(): void
    {
        $otherConflictBody = (string) json_encode([
            'code' => 'storage.buckets.alreadyExists',
            'message' => 'Bucket already exists.',
        ]);

        $mockHandler = new MockHandler([
            new Response(409, [], $otherConflictBody),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(409, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testNoRetryOn409WithInvalidJson(): void
    {
        $mockHandler = new MockHandler([
            new Response(409, [], 'not valid json'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(409, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testNoRetryOn409WithEmptyBody(): void
    {
        $mockHandler = new MockHandler([
            new Response(409, [], ''),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(409, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testNoRetryOn409WithMissingCodeField(): void
    {
        $bodyWithoutCode = (string) json_encode([
            'message' => 'Some conflict error.',
        ]);

        $mockHandler = new MockHandler([
            new Response(409, [], $bodyWithoutCode),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(409, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testNoRetryOnSuccessfulResponse(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], 'ok'),
            new Response(200, [], 'second'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(200, $response->getStatusCode());
        self::assertSame('ok', (string) $response->getBody());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testNoRetryOn4xxErrors(): void
    {
        $mockHandler = new MockHandler([
            new Response(400, [], 'Bad Request'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 3,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(400, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Second response should not be consumed');
    }

    public function testDefaultBackoffMaxTriesIsZero(): void
    {
        $mockHandler = new MockHandler([
            new Response(500, [], 'Internal Server Error'),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(500, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Should not retry when backoffMaxTries is 0');
    }

    public function testRetryOn409VersionConflictRespectsMaxRetries(): void
    {
        $versionConflictBody = (string) json_encode([
            'code' => 'storage.components.configurations.versionConflict',
            'message' => 'Configuration row creation conflict. Please retry the operation.',
        ]);

        $mockHandler = new MockHandler([
            new Response(409, [], $versionConflictBody),
            new Response(409, [], $versionConflictBody),
            new Response(409, [], $versionConflictBody),
            new Response(200, [], 'ok'),
        ]);

        $handlerStack = HandlerStack::create([
            'handler' => $mockHandler,
            'backoffMaxTries' => 2,
        ]);
        $client = new GuzzleClient(['handler' => $handlerStack, 'http_errors' => false]);

        $response = $client->request('GET', 'http://example.com');
        self::assertSame(409, $response->getStatusCode());
        self::assertSame(1, $mockHandler->count(), 'Should stop after max retries');
    }
}
